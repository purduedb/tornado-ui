/*
   Copyright 2015 - Thamir Qadah
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package ui

import spray.routing.SimpleRoutingApp
import akka.actor.ActorSystem
import spray.http.HttpHeaders._
import spray.http.ContentTypes._
import spray.http.HttpResponse
import spray.http.HttpEntity
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import spray.json.JsObject
import spray.http.ChunkedResponseStart
import spray.json.JsString
import spray.http.MessageChunk
import akka.actor.PoisonPill
import spray.json.JsNumber
import spray.http.ChunkedMessageEnd
import akka.actor.Props
import scala.concurrent.duration._
import spray.http.HttpData
import spray.http.MediaType
import spray.http.MediaTypes
import spray.http.MediaTypes._
import akka.io.Tcp
import spray.can.Http
import spray.http.HttpCharsets
import kafka.serializer._
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat
import spray.json.JsValue
import spray.json.DeserializationException
import utils.BerlinMODLatLngConverter
import utils.KafkaStringMessage
import utils.Helper
import com.typesafe.config.ConfigFactory
import java.io.File
import kafka.producer.KeyedMessage
import utils.PubSubActor
import utils.Forward
import utils.Subscribe
import utils.Unsubscribe
import spray.json.JsArray
import akka.actor.actorRef2Scala
import spray.http.ContentType.apply
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.directives.ParamDefMagnet.apply
import utils.AppJsonProtocol._
import utils._
import spray.routing.authentication.BasicAuth
import spray.routing.authentication.UserPass
import scala.concurrent.Future
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.collection.JavaConversions._
import spray.json.JsonParser
import com.turn.platform.cheetah.partitioning.horizontal._
import scala.io.Source

object Webserver extends App with SimpleRoutingApp {

  object WebserverJsonFormat extends DefaultJsonProtocol

  val conf = Helper.getConfig()

  val asystem = ActorSystem(conf.getString("webserver.actorSystem.name"),
    Helper.getActorSystemConfig(conf.getString("webserver.hostname"),
      conf.getString("webserver.actorSystem.port")))

  implicit val system = ActorSystem("webserver")
  implicit val ec = system.dispatcher
  implicit val log = system.log

  val host = conf.getString("webserver.hostname")
  val port = conf.getInt("webserver.port")

  val queryPubSubActor = asystem.actorOf(Props(classOf[PubSubActor[String]]), conf.getString("webserver.queryPubsubActorName"))
  log.info("created query pubsub service at : " + queryPubSubActor.path)

  val resultPubSubActor = asystem.actorOf(Props(classOf[PubSubActor[String]]), conf.getString("webserver.resultPubsubActorName"))
  log.info("created result pubsub service at : " + resultPubSubActor.path)

  val mbrQueries = scala.collection.Map[String, MBR]()

  val EventStreamType = register(
    MediaType.custom(
      mainType = "text",
      subType = "event-stream",
      compressible = true,
      binary = false))

  case class StreamSourceMetaData(name: String, filepath: String, port: Int, var count: Int, aref: ActorRef) {
    def rate = count
    def getActorRef = aref
    def getJsonMap = Map("name" -> JsString(name),
      "file" -> JsString(filepath),
      "host" -> JsString(host),
      "port" -> JsNumber(port),
      "rate" -> JsNumber(rate))
    override def toString = Array(name, filepath, port, count).mkString(",")
  }
  val activeStreams = scala.collection.mutable.Map[String, StreamSourceMetaData]()
  val adminSessions = ArrayBuffer[String]()

  def UPAuthenticator(userPass: Option[UserPass]): Future[Option[String]] =
    Future {
      if (userPass.exists(up => up.user == "thamir" && up.pass == "$treams")) {
        val skey = Random.nextString(10)
        //        log.info("Generated : " + skey)
        //        adminSessions += skey
        Some("thamir")
        //        Some(skey)
      } else None
    }

  val getAvailableDataFiles = path("datafiles") {
    get {
      val datafiles = new File(conf.getString("webserver.data.dir"))
      complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
        JsArray(datafiles.listFiles().filter {
          f => !f.isDirectory && """.*\.csv""".r.findFirstIn(f.getName()).isDefined
        }.map(f => JsString(f.getName()))
          .toVector).toString)))
    }
  }

  val getAllStreamSourcesRoute = path("streamsources") {
    get {
      complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
        JsArray(activeStreams.values.map {
          s => JsObject(s.getJsonMap)
        }.toVector).toString)))
    }
  }
  
  val streamSourcesControlRoute =
    sealRoute {
      path("stream-source-control") {
        //        log.info("got here before authentication")
        authenticate(BasicAuth(UPAuthenticator _, realm = "secure site")) { userName =>
          post {
            //            log.info("got here")
            formFields('name, 'filename, 'sport.as[Int], 'scount.as[Int]) { (name, fname, port, count) =>
              {
                // create source stream if not exist
                if (activeStreams.keys.toSet[String].contains(name)) {
                  complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
                    JsObject("streamName" -> JsString(name),
                      "status" -> JsString("already exists")).toString)))
                } else {
                  val portSet = activeStreams.values.flatMap(p => {
                    if (p.port == port) Some(port) else None
                  })
                  if (portSet.size > 0) {
                    log.info("port already in use")
                    complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
                      JsObject("streamName" -> JsString(name),
                        "status" -> JsString("port already in use")).toString)))
                  } else {
                    val c = system.actorOf(Props(classOf[NetworkSocketControllerServer], conf.getString("webserver.data.dir") + "/" + fname, host, port, count, 1), name = name)
                    val ss = StreamSourceMetaData(name, fname, port.toInt, count.toInt, c)
                    log.info("created a stream source with: " + ss.toString)
                    activeStreams += (name -> ss)
                    complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
                      JsObject("streamName" -> JsString(name),
                        "status" -> JsString("created")).toString)))
                  }

                }
              }
            } ~
              formFields('name, 'scount.as[Int]) { (name, count) =>
                {
                  // update rate 
                  if (activeStreams.keys.toSet[String].contains(name)) {

                    val s = activeStreams.get(name)

                    s.foreach(ss => {
                      ss.count = count
                      ss.aref ! utils.ChangeCount(count)
                    })
                    complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
                      JsObject("streamName" -> JsString(name),
                        "status" -> JsString("updated")).toString)))
                  } else {
                    complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
                      JsObject("streamName" -> JsString(name),
                        "status" -> JsString("does not exist")).toString)))
                  }
                }
              }
          } ~
            delete {
              parameters('name) {
                (name) =>
                  {
                    activeStreams -= name
                    complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
                      JsObject("streamName" -> JsString(name),
                        "status" -> JsString("deleted")).toString)))
                  }

              }
            }
        }
      }
    }

  // Tornado related code
  
  // Demo mode
  
  // OSM load 
  // inverted index of tags  
  // keys will be tags and values will be list of spatial objects
  
  val osm_data = scala.io.Source.fromFile("data/chicago-osm.json").getLines.map { JsonParser(_).asJsObject };

  
  val osmtags = scala.collection.mutable.Map[String, ArrayBuffer[String]]()
  
  //Twitter data load
  val tweets = scala.io.Source.fromFile("data/tweet_us_2013_1_3.chicago.csv").getLines()
  val trate = 10;

  // Moving object data
  val mod = 2;
  
//  val tornado_ds_map = new  
  
  val tornado_datasources = path("tornado/datasources"){
    get {
      complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
        JsArray(JsObject("sourceName" -> JsString("Tweets")),
            JsObject("sourceName" -> JsString("OSMData")),
            JsObject("sourceName" -> JsString("BerlinMOD"))
            ).toString)))
    }
  }
  
  val tornado_queries = path("tornado/queries"){
    get {
      complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
        JsArray(activeStreams.values.map {
          s => JsObject(s.getJsonMap)
        }.toVector).toString)))
    }
  }

  
  
  /// end Demo mode
  
  // adaptive index visualization test
  import experiments._

  val costEstimator = WebDemo2.getDefaultCostEstimator()
  var solution = new Solution()
  val k = 100
  WebDemo2.dynamic = new DynamicPartitioning(costEstimator, k, 1000)
  //WebDemo2.solvePartitioning(solution, WebDemo2.dynamic.initialPartitions());
  WebDemo2.dynamic.initialPartitions()

  val aqwaqueries = WebDemo2.getSerialQLoad()
  var i: Int = 0
  var p: Partition = aqwaqueries(i)

  // mapping demo to latlng 
  val xmap = scala.collection.mutable.Map[Int, Double]()
  val ymap = scala.collection.mutable.Map[Int, Double]()

  var cnorth: String = ""
  var csouth: String = ""
  var ceast: String = ""
  var cwest: String = ""

  
  startServer(interface = host, port = port) {
    getFromDirectory("ui/public") ~
      path("data") {
        parameters('north, 'west, 'south, 'east) { (n, w, s, e) =>
          {
            get {
              complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, """ {"key":"value"} """)))
            }
          }
        }
      } ~
      path("chicago-osm") {
        get {
          //          val chiosm = Source.fromFile("data/chicago-osm.json").getLines().toArray.filter(_.contains("attraction"))
          //          val res1 = chiosm.map{ attr => {
          //            
          //            val attr_obj = JsonParser(attr)
          //            val lat = attr_obj.asJsObject.fields.get("lat").get
          //            val lng = attr_obj.asJsObject.fields.get("lng").get
          //            
          //            }
          //            
          //          }

          //          val jres =  tweets.take(trate).flatMap { tw => {
          //              val tarr = tw.split(",")
          //              val tlat = tarr(2)
          //              val tlng = tarr(3)
          //              val ttext = tarr(5)
          //              if (ttext.contains("SkyDec")) Some(JsObject("attraction" ->  
          //                  "tweet"))
          //              None
          //            } }
          //          
          //          val res = "["+chiosm.mkString(",")+"]"
          //          

          complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, "")))
        }
      } ~
      path("osmdata") {
        get {
          complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, """ {"key":"value"} """)))
        }
      } ~
      path("twitter-stream") {
        ctx =>
          {
            val aprops = Props(classOf[KafkaTopicStreamer], ctx.responder, "inputTweets", (Helper.formatSSETweets _), EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      } ~
      path("twitter-output-stream") {
        ctx =>
          {
            //            val aprops = Props(classOf[KafkaTopicStreamer], ctx.responder, "output", (formatSSETweets _), EventStreamType)
            //            val aprops = Props(classOf[KafkaTopicStreamer], ctx.responder, "output", (formatSSETweets _), EventStreamType)
            log.info("Creating a subscription to " + resultPubSubActor)
            val aprops = Props(classOf[BufferingStreamer], ctx.responder, resultPubSubActor, (Helper.formatSSETweets _), EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      } ~
      path("output-stream") {
        ctx =>
          {
            log.info("Creating a subscription to " + resultPubSubActor)
            val aprops = Props(classOf[BufferingStreamer], ctx.responder, resultPubSubActor, (Helper.selfString _), EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      } ~
      path("new-range-query") {
        parameters('qid, 'north, 'west, 'south, 'east) { (qid, n, w, s, e) =>
          {
            ctx =>
              {
                //	            qid=1&north=32.84267363195431&west=31.9921875&south=11.867350911459308&east=60.46875ok query-tweetsvis.htm:127
                //received an sse : "{\"type\":\"output\",\"data\":{\"lng\":29.093488,\"timestamp\":\"2014-10-02 15:52:19.000\",\"text\":\"@NoysArt what? Umh NoyEliyahu?\",\"id\":517764033655869441,\"lat\":41.028209}} " 
                //	        	  val q = Array("+",qid,n+":"+w,s+":"+e).mkString(",")
                val q = Array(qid, e + ":" + n, w + ":" + s).mkString(",")
                log.info("registring new range query : " + q)

                queryPubSubActor ! Forward(q)

                //	        	  val producer = Helper.createKafkaProducer
                //	        	  val data = new KeyedMessage[String, String]("queries", q)
                //	        	  producer.send(data)
                //	        	  producer.close
                ctx.complete("ok")
              }
          }
        }
      } ~
      path("delete-range-query") {
        parameters('qid) { (qid) =>
          {
            ctx =>
              {
                val q = Array("-", qid).mkString(",")
                log.info("delete range query : " + q)
                val producer = Helper.createKafkaProducer
                val data = new KeyedMessage[String, String]("queries", q)
                producer.send(data)
                producer.close
                ctx.complete("ok")
              }
          }
        }
      } ~
      path("query-stream") {
        ctx =>
          {
            val aprops = Props(classOf[KafkaTopicStreamer], ctx.responder, "queries", (Helper.selfString _), EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      } ~
      path("input-stream") {
        ctx =>
          {
            val aprops = Props(classOf[KafkaTopicStreamer], ctx.responder, "input", (Helper.selfString _), EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      } ~
      path("output-stream") {
        ctx =>
          {
            val aprops = Props(classOf[KafkaTopicStreamer], ctx.responder, "output", (Helper.selfString _), EventStreamType)
            val streamer = system.actorOf(aprops)

          }
      } ~
      path("answer-stream") {
        ctx =>
          {
            val aprops = Props(classOf[Streamer], ctx.responder, "output")
            val streamer = system.actorOf(aprops)

          }
      } ~
      path("berlinmod-query-stream") {
        ctx =>
          {
            val datafile = conf.getString("webserver.data.queries")
            val aprops = Props(classOf[BerlinMODQueryStreamer], ctx.responder, datafile, EventStreamType)
            val streamer = system.actorOf(aprops)

          }
      } ~
      path("twitter-range-query-stream") {
        ctx =>
          {
            val datafile = conf.getString("webserver.data.twitter.queries.range")
            val aprops = Props(classOf[TwitterQueryStreamer], ctx.responder, datafile, "range", EventStreamType)
            val streamer = system.actorOf(aprops)

          }
      } ~
      path("twitter-knn-query-stream") {
        ctx =>
          {
            val datafile = conf.getString("webserver.data.twitter.queries.knn")
            val aprops = Props(classOf[TwitterQueryStreamer], ctx.responder, datafile, "knn", EventStreamType)
            val streamer = system.actorOf(aprops)

          }
      } ~
      path("adaptive") {
        parameters('north, 'west, 'south, 'east) { (n, w, s, e) =>
          {
            ctx =>
              {

                val side = 1000
                var recompute_mapping = false
                if (cnorth == "") {
                  // fisrst request
                  recompute_mapping = true
                } else {
                  //view bounds changed
                  if (cnorth != n || csouth != s || cwest != w || ceast != e) recompute_mapping = true
                }

                if (recompute_mapping) {
                  //            println(s"got a request for $n , $w , $s , $e")
                  val minx = w.toDouble
                  val miny = s.toDouble
                  val rx = (e.toDouble - minx)
                  val ry = n.toDouble - miny
                  //            println(s"got a request for minx($minx) , miny($miny) , rangex($rx) , rangey($ry)")

                  for (i <- 0 to side) {
                    xmap.put(i, minx + (i * (rx / side)))
                    ymap.put(i, miny + (i * (ry / side)))
                  }

                  cnorth = n; csouth = s; cwest = w; ceast = e;
                }

                // process 

                //            if (i % 100 ==0) {
                //
                //                WebDemo2.lines.clear()
                //                WebDemo2.addRectangle(p);
                //                solution = new Solution();
                //                WebDemo2.solvePartitioning(solution, WebDemo2.dynamic.currentPartitions);
                //                WebDemo2.updatePartitionsSolution(solution);
                //                WebDemo2.updatePartitions(solution.toString());
                //                
                //            }
                WebDemo2.dynamic.processNewQuery(aqwaqueries(i));

                val ps = scala.collection.mutable.ArrayBuffer[Partition]()
                ps ++= WebDemo2.dynamic.currentPartitions

                // send query load    
                //            val res = aqwaqueries.subList(0, 100).map{p => {
                //              JsObject("north" -> JsNumber(ymap.get(p.getTop()).get)
                //                       ,"south" -> JsNumber(ymap.get(p.getBottom()).get)
                //                       ,"west" -> JsNumber(xmap.get(p.getLeft()).get)
                //                       ,"east" -> JsNumber(xmap.get(p.getRight()).get)
                //                       ,"color" -> JsString("#FF0000")
                //                      )
                //            }}

                val res = ps.map { p =>
                  {
                    JsObject("north" -> JsNumber(ymap.get(p.getTop()).get), "south" -> JsNumber(ymap.get(p.getBottom()).get), "west" -> JsNumber(xmap.get(p.getLeft()).get), "east" -> JsNumber(xmap.get(p.getRight()).get), "color" -> JsString("#000000"))
                  }
                }
                // add query here
                res += JsObject("north" -> JsNumber(ymap.get(aqwaqueries(i).getTop()).get), "south" -> JsNumber(ymap.get(aqwaqueries(i).getBottom()).get), "west" -> JsNumber(xmap.get(aqwaqueries(i).getLeft()).get), "east" -> JsNumber(xmap.get(aqwaqueries(i).getRight()).get), "color" -> JsString("#FF0000"))

                i = i + 1
                if (i == aqwaqueries.size()) i = 0;
                p = aqwaqueries(i)

                val resp = JsArray(res.toVector)
                //            println("sending out "+res.size+" rects")
                //            println(resp.toString())
                val hres = HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, resp.toString()))
                ctx.complete(hres)
              }
          }
        }
      } ~
      path("berlinmod-trip-stream") {
        ctx =>
          {
            val datafile = conf.getString("webserver.data.trips")
            val datatype = conf.getString("webserver.data.trips-type")
            val freq = conf.getString("webserver.data.freq").toInt
            val aprops = Props(classOf[BerlinMODTripStreamer], ctx.responder, datafile, datatype, freq, EventStreamType)
            val streamer = system.actorOf(aprops)

          }
      } ~
      path("mbr-stream") {
        parameters('north, 'west, 'south, 'east) { (n, w, s, e) =>
          {
            ctx =>
              {

                val aprops = Props(classOf[MBRStreamer], ctx.responder, new MBR(n.toDouble, w.toDouble, s.toDouble, e.toDouble), EventStreamType)
                val streamer = system.actorOf(aprops)
              }
          }
        }
      } ~
      path("stream") {
        ctx =>
          {
            val aprops = Props(classOf[Streamer], ctx.responder, EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      }~
      getAvailableDataFiles ~
      getAllStreamSourcesRoute //~
      //streamSourcesControlRoute
  }
}