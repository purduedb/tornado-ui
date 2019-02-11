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
                if (activeStreams.keys.toSet[String].contains(name)){
                  complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
				        JsObject("streamName"-> JsString(name),
				        		"status" -> JsString("already exists")).toString)))   	                
                }               
                else{
                  val portSet = activeStreams.values.flatMap(p => {
                    if (p.port == port) Some(port) else None
                  })
                  if (portSet.size > 0){
                    log.info("port already in use")
                	  complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
				        JsObject("streamName"-> JsString(name),
				        		"status" -> JsString("port already in use")).toString)))  
                  }
                  else{
                    val c = system.actorOf(Props(classOf[NetworkSocketControllerServer], conf.getString("webserver.data.dir")+"/"+fname, host, port, count, 1), name = name)                
	                val ss = StreamSourceMetaData(name, fname, port.toInt, count.toInt, c)
	                log.info("created a stream source with: "+ss.toString)
	                activeStreams += (name -> ss)
	                complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
				        JsObject("streamName"-> JsString(name),
				        		"status" -> JsString("created")).toString)))   
                  }
                  
	            }                 
              }
            }~
            formFields('name, 'scount.as[Int]) { (name, count) =>{
              // update rate 
              if (activeStreams.keys.toSet[String].contains(name)){
                
            	  val s = activeStreams.get(name)
            	  
            	  s.foreach(ss => {
            	    ss.count = count
            	    ss.aref ! utils.ChangeCount(count)
            	  })
                  complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
				        JsObject("streamName"-> JsString(name),
				        		"status" -> JsString("updated")).toString)))   	                
                }
                else{                  
	                complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
				        JsObject("streamName"-> JsString(name),
				        		"status" -> JsString("does not exist")).toString)))   
	            }
            }
            }
          }~
          delete{
            parameters('name){ 
              (name) => {
                activeStreams -= name
                complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
			        JsObject("streamName"-> JsString(name),
			        		"status" -> JsString("deleted")).toString)))
              }
              
            }                
          }
        }
      }
    }

  startServer(interface = host, port = port) {
    getFromDirectory("ui/public") ~
      getAvailableDataFiles ~
      getAllStreamSourcesRoute ~
      streamSourcesControlRoute ~
      path("data") {
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
      }

  }
}