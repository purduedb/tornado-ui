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

package tornado.ui

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
import ui._
import ui.KafkaTopicStreamer
import spray.http.StatusCodes
import org.apache.kafka.clients.producer.ProducerRecord
import spray.json.JsBoolean
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout

object TornadoWebserver extends App with SimpleRoutingApp {

  object TornadoWebserverJsonFormat extends DefaultJsonProtocol

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
  
  var kafkaOutputPubSub: ActorRef = null
  var kafkaAdaptivePubSub: ActorRef = null
  
  if (Helper.getConfig().getBoolean("kafka.enabled")){
    kafkaOutputPubSub = asystem.actorOf(Props(classOf[KafkaPubSub],  conf.getString("kafka.zk") , "outputgroup", conf.getString("kafka.topics.output")), "pubsub-output")
    log.info("created kafka pubsub service for topic("+conf.getString("kafka.topics.output")+") at : " + kafkaOutputPubSub.path)
    
    kafkaAdaptivePubSub = asystem.actorOf(Props(classOf[KafkaPubSub], conf.getString("kafka.zk"), "aigroup", conf.getString("kafka.topics.adaptiveIndexUpdates")), "pubsub-adaptive")
    log.info("created kafka pubsub service for topic("+conf.getString("kafka.topics.adaptiveIndexUpdates")+") at : " + kafkaOutputPubSub.path)  
  }
  
  
  val EventStreamType = register(
    MediaType.custom(
      mainType = "text",
      subType = "event-stream",
      compressible = true,
      binary = false))

      
  def createStatusResponse(status:String, msg:String) = {
    JsObject("status" -> JsString(status) , "msg" -> JsString(msg) ).toString()
  }    
  // Tornado related code

  // Demo mode

  // OSM load 
  // inverted index of tags  
  // keys will be tags and values will be list of spatial objects

  //val osm_data = scala.io.Source.fromFile("data/chicago-osm.json").getLines.map { JsonParser(_).asJsObject };
//  Catalog.datasources += ("OSM_Data" -> LocalFileDataSource("chicago-osm","data/chicago-osm.json"))
  
    //Twitter data load
//  Catalog.datasources += ("HisTwitter" -> LocalFileDataSource("twitter_chicago","data/tweet_us_2013_1_3.chicago.csv"))
  
//  val tweets = scala.io.Source.fromFile("data/tweet_us_2013_1_3.chicago.csv").getLines()
  val trate = 10;

  // Moving object data
  val mod = 2;
  
 

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

  //  val tornado_ds_map = new  

  val tornado_datasources = pathPrefix("tornado") {
    path("datasources") {
      get {
        complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
          JsArray(JsObject("sourceName" -> JsString("Tweets")),
            JsObject("sourceName" -> JsString("POI_Data")),
            JsObject("sourceName" -> JsString("MovingObjects"))).toString)))
      }
    }
  }

  val registeredQueries = Map[String,JsObject]()
  
  val tornado_output_cqueries = path("tornado" / "output" / "cqueries" / Segment){ 
       qname =>{
         var resp: String = ""
         Catalog.json_cqueries.get(qname) match {
           case Some(q) => {
//             log.info("Got a request for results of : "+q.toString())
             complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
                q.toString())))
             
           } 
           case None => {
            complete(StatusCodes.NotFound)
           }
         }
         
       }}
  
  def publishToKafka(qname:String,jstr:String) = {
    if (Helper.getConfig().getBoolean("kafka.enabled")) {
        val rec = new ProducerRecord[String,String](Helper.getConfig().getString("kafka.producer.topic"),qname,jstr)
        Catalog.KafkaProducerSend(rec)  
      }
  }
  
  def publishToKafkaQueries(qname:String,jstr:String) = {
    publishToKafkaTopic(qname, jstr, Helper.getConfig().getString("kafka.topics.queries"))
  }
  
  def publishToKafkaHotspots(k:String,v:String) = {
    publishToKafkaTopic(k, v, Helper.getConfig().getString("kafka.topics.hotspots"))
  }
  
  def publishToKafkaTopic(k:String,v:String, topic : String) = {
    if (Helper.getConfig().getBoolean("kafka.enabled")) {
        val rec = new ProducerRecord[String,String](topic,k,v)
        Catalog.KafkaProducerSend(rec)  
      }
  }
  
  val tornado_queries = path("tornado" / "queries") {
      get {
        complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
          Catalog.getRegisteredCQueries.toString())))
      }~
      post{
        ctx => {       
          var ret:String = ""
          val jstr = ctx.request.entity.asString
          val jo = JsonParser(jstr)
//          log.info("received request, entity = " + jo.prettyPrint)
          log.info(jo.prettyPrint)
          val qname = jo.asJsObject.fields.get("name").get.asInstanceOf[JsString].value
          
            jo.asJsObject.fields.get("type") match {
            case Some(JsString(cq)) => {
              cq match {
                case "cquery" => {
                  // continuous query
                  
                  Catalog.json_cqueries.get(qname) match {
                    case Some(qn) =>{
                      ret = createStatusResponse("error","query is already registered : "+qname)
                    }
                    case None => {
                      // register query
                      Catalog.json_cqueries += (qname -> jo.asJsObject)
                      
                      // publish to Kafka if needed
                      
                      publishToKafkaQueries(qname,jstr)
                      
                      ret = createStatusResponse("success","query registered : "+qname)
                    }
                  }
                  
                  
                  
                }
                case "squery" => {
                  // snapshot query and static queries
                  ret = createStatusResponse("error","not handling snapshot/static queris for now")
                }
                case _ => {
                  ret = createStatusResponse("error","unknown query type : "+cq)
                }
              }
            }
          }
          
          ctx.complete(ret)
        }
      }~
      delete {
        ctx => {
          val jstr = ctx.request.entity.asString
          val jo = JsonParser(jstr)
          log.info(jo.prettyPrint)
          var resp = ""
          val qname = jo.asJsObject.fields.get("name") 
          qname match {
            case Some(x) => {
              val key = x.asInstanceOf[JsString].value
              
              Catalog.json_cqueries.remove(key) match {
                case Some(o) => {
                  resp = createStatusResponse("success", "successfully removed query : "+key)
                }
                case None => {
                  resp = createStatusResponse("error", "cannot remove nonexistent query : "+key)
                }
              }
              // publish to kafka if needed
              publishToKafkaQueries(key, jstr)
            }
            case None => {
              resp = createStatusResponse("error", "malformed/invalid delete request ")
            }
          }
          
          ctx.complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, resp)))
        }
      }
  }

  val serverMessages = path("tornado"  / "stream" / "servermsgs") {
        complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
          "{\"status\":\"ok\"}")))     
  }
  
  val systemConfig = path("tornado" / "config") {
    val kafkaConf = JsObject("enabled" -> JsBoolean(Helper.getConfig().getBoolean("kafka.enabled")))
    val resp = JsObject("kafka" -> kafkaConf)
    complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
          resp.toString())))
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
  
  // partitioning data for adaptive index mockup
  val utestdata = Helper.getConfig().getString("data.adaptiveIndexMock")
  // e.g. "data/PartitionsJSON.txt" 
  var pupdates = Source.fromFile(utestdata).getLines();
    

  startServer(interface = host, port = port) {
    getFromDirectory("ui/public") ~
      tornado_datasources ~
      tornado_queries ~
      tornado_output_cqueries ~
      serverMessages ~
      systemConfig ~
      path("data") {
        parameters('north, 'west, 'south, 'east) { (n, w, s, e) =>
          {
            get {
              complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, """ {"key":"value"} """)))
            }
          }
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
      path("mock-output-stream") {
        ctx =>
          {
            log.info("Mocking an output stream")
            val aprops = Props(classOf[RandomPointWithTextStreamer], ctx.responder, EventStreamType)
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
      path("kafka" / "query-stream") {
        ctx =>
          {
            val aprops = Props(classOf[KafkaTopicStreamer], ctx.responder, Helper.getConfig().getString("kafka.topics.queries"), (Helper.selfString _), EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      } ~
      path("kafka" / "input-stream") {
        ctx =>
          {
            val aprops = Props(classOf[KafkaTopicStreamer], ctx.responder, "input", (Helper.selfString _), EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      } ~
      path("kafka" / "output-stream2") {
        ctx =>
          {
            if (Helper.getConfig().getBoolean("kafka.enabled")){
              log.info("Kafka output stream")
              val aprops = Props(classOf[KafkaActorStreamer], ctx.responder, kafkaOutputPubSub, Helper.getConfig().getString("kafka.topics.output"), (Helper.selfString _), EventStreamType)
              val streamer = system.actorOf(aprops)  
            }
            else {
              ctx.complete(StatusCodes.NotFound)
            }
          }
      } ~      
      path("kafka" / "adaptiveIndex") {
        ctx =>
          {
            if (Helper.getConfig().getBoolean("kafka.enabled")){
              implicit val timeout = Timeout(5 seconds)
//            log.info("======================= Kafka adaptive stream")
              val fmsg = kafkaAdaptivePubSub ? GetLastMessage 
              val result = Await.result(fmsg, timeout.duration).asInstanceOf[KafkaStringMessage]
              ctx.complete(result.x)
            }    
            else {
              ctx.complete(StatusCodes.NotFound)
            }
          }
      } ~      
      path("adaptiveIndex") {
        get {
            ctx =>
              {
                
                if (!pupdates.hasNext) {
                  log.info("repeat update cycle")
                  pupdates = Source.fromFile(utestdata).getLines()
                }
                
                val hres = HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, pupdates.next()))
                ctx.complete(hres)          
              }
         }
      } ~
      path("hotspot") {
        post {
          ctx =>
          {
            val jstr = ctx.request.entity.asString
            log.info("got: "+jstr)
            
            publishToKafkaHotspots("adaptive", jstr)
            
            ctx.complete("got hotspot info")     
          }
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
                
                // process 
                
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
                
                // create list of rectangles
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
      getAvailableDataFiles
  }
}