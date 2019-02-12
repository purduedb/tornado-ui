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
import spray.json.JsonParser
import tornado.ui.Catalog

object Ready
object SendReady

class KafkaTopicStreamer(peer: ActorRef, topic: String, formatSSE: String => JsValue, EventStreamType: MediaType) extends Actor with ActorLogging {

  val id = Catalog.kafkaConsumerCount.incrementAndGet()
  val max = 200

  val kafkaQueue = scala.collection.mutable.Queue[String]()

  val conf = ConfigFactory.parseFile(new File("application.conf"))

  val zk = conf.getString("kafka.zk")
  val cgid = "cgid-web-" + id

  context.watch(peer)

  val consumer = utils.KafkaConsumerHelper.startKafkaConsumer(zk, cgid, topic, this.context.system, self)

  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(SendReady)

  implicit val ec = this.context.system.dispatcher

  var waiting = false

  def receive = {
    case Ready => {
      this.context.system.scheduler.scheduleOnce(500 milliseconds, self, SendReady)
    }
    case SendReady => {

      if (kafkaQueue.size == 0) waiting = true

      while (kafkaQueue.size > 0) {
        val resp = JsObject("type" -> JsString(topic), "data" -> formatSSE(kafkaQueue.dequeue))
        val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString()))
        peer ! newChunk.withAck(SendReady)

      }

    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      consumer ! PoisonPill
      self ! PoisonPill

    }

    case Tcp.Abort | Tcp.Aborted => {
      log.info("Peer aborted, ending respnse streaming ")
      consumer ! PoisonPill
      self ! PoisonPill
    }

    case KafkaStringMessage(kafkaMsg) => {
      kafkaQueue.enqueue(kafkaMsg)
      //        log.info("Receieved kafka message: " + kafkaMsg)

      if (waiting && kafkaQueue.size == 1) {
        self ! SendReady
      }
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class KafkaTopicStreamerWithFiltering(peer: ActorRef, topic: String, formatSSE: String => JsValue, EventStreamType: MediaType) extends Actor with ActorLogging {

  val id = Catalog.kafkaConsumerCount.incrementAndGet()
  val max = 200

  val kafkaQueue = scala.collection.mutable.Queue[String]()

  val conf = ConfigFactory.parseFile(new File("application.conf"))

  val zk = conf.getString("kafka.zk")
  val cgid = "cgid-web-" + id

  context.watch(peer)

  val consumer = utils.KafkaConsumerHelper.startKafkaConsumer(zk, cgid, topic, this.context.system, self)

  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(SendReady)

  implicit val ec = this.context.system.dispatcher

  var waiting = false

  def receive = {
    case Ready => {
      this.context.system.scheduler.scheduleOnce(500 milliseconds, self, SendReady)
    }
    case SendReady => {

      if (kafkaQueue.size == 0) waiting = true

      while (kafkaQueue.size > 0) {
        val outputTuple = kafkaQueue.dequeue
        try{
          val jo = JsonParser(outputTuple).asJsObject
          val tqname = jo.fields.get("name").asInstanceOf[JsString].value
          Catalog.json_cqueries.get(tqname) match {
            case Some(qjo) =>{
              val resp = JsObject("type" -> JsString(topic), "data" -> formatSSE(outputTuple))        
              val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString()))
              peer ! newChunk.withAck(SendReady)   
            }
            case None => {
              log.info("received ouput for de-registered query ("+tqname+"), ignoring it")
            }
          }            
        }
        catch {
          case e:Throwable => {
            log.info(e.getMessage)
          }
        }
        
      }

    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      consumer ! PoisonPill
      self ! PoisonPill

    }

    case Tcp.Abort | Tcp.Aborted => {
      log.info("Peer aborted, ending respnse streaming ")
      consumer ! PoisonPill
      self ! PoisonPill
    }

    case KafkaStringMessage(kafkaMsg) => {
      kafkaQueue.enqueue(kafkaMsg)
      //        log.info("Receieved kafka message: " + kafkaMsg)

      if (waiting && kafkaQueue.size == 1) {
        self ! SendReady
      }
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class KafkaActorStreamer(peer: ActorRef, kconsumer: ActorRef, topic: String, formatSSE: String => JsValue, EventStreamType: MediaType) extends Actor with ActorLogging {

  val kafkaQueue = scala.collection.mutable.Queue[String]()

  val conf = ConfigFactory.parseFile(new File("application.conf"))

  context.watch(peer)

  kconsumer ! SubscribeStreamer(self)
  
  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(SendReady)
  
  

  implicit val ec = this.context.system.dispatcher

  var waiting = false

  def receive = {
    case Ready => {
      this.context.system.scheduler.scheduleOnce(500 milliseconds, self, SendReady)
    }
    case SendReady => {

      if (kafkaQueue.size == 0) waiting = true

      while (kafkaQueue.size > 0) {
        val resp = JsObject("type" -> JsString(topic), "data" -> formatSSE(kafkaQueue.dequeue))
        val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString()))
        peer ! newChunk.withAck(SendReady)

      }

    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      kconsumer ! UnsubscribeStreamer(self)
      self ! PoisonPill

    }

    case Tcp.Abort | Tcp.Aborted => {
      log.info("Peer aborted, ending respnse streaming ")
      kconsumer ! UnsubscribeStreamer(self)
      self ! PoisonPill
    }

    case KafkaStringMessage(kafkaMsg) => {
      kafkaQueue.enqueue(kafkaMsg)
      //        log.info("Receieved kafka message: " + kafkaMsg)

      if (waiting && kafkaQueue.size == 1) {
        self ! SendReady
      }
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class RandomPointWithTextStreamer(peer: ActorRef, EventStreamType: MediaType) extends Actor with ActorLogging {

  log.info("starting respnse streaming ")
  val sampleTweetsFile = Helper.getConfig().getString("webserver.data.sampleTweets")
  var data = scala.io.Source.fromFile(sampleTweetsFile).getLines()

  context.watch(peer)

  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(Ready)

  implicit val ec = this.context.system.dispatcher

  def receive = {
    case Ready => {
      this.context.system.scheduler.scheduleOnce(500 milliseconds, self, SendReady)
    }
    case SendReady => {
      val qn = Catalog.json_cqueries.keys.size
      if (qn > 0) {
        val qname = Catalog.json_cqueries.keys.toList(scala.util.Random.nextInt(qn))
        val cv = Catalog.json_cqueries.get(qname).get.fields.get("currentView").get
        val qcolor = Catalog.json_cqueries.get(qname).get.fields.get("outputColor").get
        val bounds = cv.asJsObject.getFields("north", "west", "south", "east").map { _.asInstanceOf[JsNumber].value }
        val rlng = bounds(1).toDouble + (scala.math.abs(bounds(3).toDouble - bounds(1).toDouble) * scala.util.Random.nextDouble())
        val rlat = bounds(2).toDouble + (scala.math.abs(bounds(0).toDouble - bounds(2).toDouble) * scala.util.Random.nextDouble())

        var txt= ""
        if (data.hasNext) {
          txt = data.next().split(",")(5)
        }
        else {
          data =  scala.io.Source.fromFile(sampleTweetsFile).getLines()
        }
        
        val resp = JsObject("name" -> JsString(qname),
          "type" -> JsString("output"),
          "outputColor" -> qcolor,
          "point" -> JsObject("lat" -> JsNumber(rlat), "lng" -> JsNumber(rlng)),
          "text" -> JsString(txt))

        val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString))

        peer ! newChunk.withAck(Ready)

      } else {
        this.context.system.scheduler.scheduleOnce(900 milliseconds, self, Ready)
      }

    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      self ! PoisonPill
    }
    
//    case Tcp.Aborted => {
//      log.info("Responder Peer has aborted, ending streamer instance ")
//      self ! PoisonPill
//    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class MBRStreamer(peer: ActorRef, mbr: MBR, EventStreamType: MediaType) extends Actor with ActorLogging {

  log.info("starting respnse streaming ")

  var count = 0
  val max = 200

  context.watch(peer)

  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(Ready)

  implicit val ec = this.context.system.dispatcher

  def receive = {
    case Ready => {
      this.context.system.scheduler.scheduleOnce(500 milliseconds, self, SendReady)
    }
    case SendReady => {

      val rlat = (Math.random() * (mbr.north.abs - mbr.south.abs).abs) + mbr.south
      val rlng = (Math.random() * (mbr.west.abs - mbr.east.abs).abs) + mbr.west

      //        val rlat = (Math.random())
      //        val rlng = (Math.random())

      val resp = JsObject("type" -> JsString("location"), "lat" -> JsNumber(rlat), "lng" -> JsNumber(rlng))
      val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString))

      peer ! newChunk.withAck(Ready)

      if (count > max) {
        peer ! ChunkedMessageEnd()
        log.info("Max count reached, ending respnse streaming ")
      }

      count += 1
    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      self ! PoisonPill
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class Streamer(peer: ActorRef, EventStreamType: MediaType) extends Actor with ActorLogging {

  log.info("starting respnse streaming ")

  var count = 0
  val max = 200

  context.watch(peer)

  val streamStart = JsObject("status" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(Ready)

  implicit val ec = this.context.system.dispatcher

  def receive = {
    case Ready => {
      this.context.system.scheduler.scheduleOnce(500 milliseconds, self, SendReady)
    }
    case SendReady => {

      //	    val rlat = (Math.random()*(Math.abs(mbr.north-mbr.south)))+mbr.south
      //	    val rlng = (Math.random()*(Math.abs(mbr.west-mbr.east)))+mbr.east

      val rlat = (Math.random())
      val rlng = (Math.random())

      val resp = JsObject("type" -> JsString("location"), "lat" -> JsNumber(rlat), "lng" -> JsNumber(rlng))
      val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString))

      peer ! newChunk.withAck(Ready)

      if (count > max) {
        peer ! ChunkedMessageEnd()
        log.info("Max count reached, ending respnse streaming ")
      }

      count += 1
    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      self ! PoisonPill
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class BerlinMODTripStreamer(peer: ActorRef, filepath: String, dataType: String, freq: Int, EventStreamType: MediaType) extends Actor with ActorLogging {

  log.info("starting respnse streaming ")

  lazy val fs = scala.io.Source.fromFile(filepath).getLines.buffered

  context.watch(peer)

  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(Ready)

  implicit val ec = this.context.system.dispatcher

  if (dataType == "trips") fs.next()

  def receive = {
    case Ready => {
      this.context.system.scheduler.scheduleOnce(freq milliseconds, self, SendReady)
    }
    case SendReady => {

      if (fs.hasNext) {
        val tup = fs.next()
        //val trip = Helper.createTrip(tup)

        val resp: JsObject = dataType match {
          case "trips" => {
            val trip = Helper.createTrip(tup)
            JsObject("type" -> JsString("trip"),
              "moid" -> JsNumber(trip._1.getId),
              "tstart" -> JsObject("ts" -> JsNumber(trip._1.t.getMillis()),
                "lat" -> JsNumber(trip._1.sobj.getLatLng._1),
                "lng" -> JsNumber(trip._1.sobj.getLatLng._2)),
              "tend" -> JsObject("ts" -> JsNumber(trip._2.t.getMillis()),
                "lat" -> JsNumber(trip._2.sobj.getLatLng._1),
                "lng" -> JsNumber(trip._2.sobj.getLatLng._2)))
          }
          case "updates" => {
            var ctup = tup
            val arr = tup.split(",")
            val objectList = scala.collection.mutable.ArrayBuffer[String]()
            while (fs.hasNext && fs.head.split(",")(1).toLong == arr(1).toLong) {
              objectList += ctup
              ctup = fs.next
            }

            val jsarr = JsArray(objectList.toVector.map(line => {
              val arr = line.split(",")
              val coord = BerlinMODLatLngConverter.getLatLng(arr(2).toDouble, arr(3).toDouble)
              JsObject("moid" -> JsNumber(arr(0)), "lat" -> JsNumber(coord._1),
                "lng" -> JsNumber(coord._2))
            }))

            JsObject("type" -> JsString("updates"),
              "ts" -> JsNumber(arr(1)),
              "updates" -> jsarr)
          }
        }

        val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString))
        peer ! newChunk.withAck(Ready)
      } else {
        val streamEnd = JsObject("type" -> JsString("stream-end"))
        val newChunk = MessageChunk(Helper.createSSE("ping", streamEnd.toString))
        peer ! newChunk
        peer ! ChunkedMessageEnd()
        log.info("EOF reached, ending respnse streaming ")

      }

    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      self ! PoisonPill
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class BerlinMODQueryStreamer(peer: ActorRef, filepath: String, EventStreamType: MediaType) extends Actor with ActorLogging {

  log.info("starting respnse streaming ")

  var marker = 1
  lazy val fs = scala.io.Source.fromFile(filepath).getLines

  // Skip the first line
  fs.next()

  var count = 0
  val max = 200

  context.watch(peer)

  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(Ready)

  implicit val ec = this.context.system.dispatcher

  def receive = {
    case Ready => {
      this.context.system.scheduler.scheduleOnce(500 milliseconds, self, SendReady)
    }
    case SendReady => {

      //        val qline = fs(marker)
      val (nmarker, q) = utils.Helper.getSimpleRangeQuery(fs, marker)

      marker = nmarker + 1

      val northwest = BerlinMODLatLngConverter.getLatLng(q._2(0).x, q._2(0).y)
      val southeast = BerlinMODLatLngConverter.getLatLng(q._2(1).x, q._2(1).y)

      val resp = JsObject("type" -> JsString("query"), "qtype" -> JsString("rect"), "north" -> JsNumber(northwest._1), "east" -> JsNumber(southeast._2), "south" -> JsNumber(southeast._1), "west" -> JsNumber(northwest._2))

      val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString))

      peer ! newChunk.withAck(Ready)

      if (count > max) {
        peer ! ChunkedMessageEnd()
        log.info("Max count reached, ending respnse streaming ")
      }

      count += 1
    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      self ! PoisonPill
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class TwitterDemoResultStreamer(peer: ActorRef, pubsub: ActorRef, formatSSE: String => String, EventStreamType: MediaType) extends Actor with ActorLogging {

  log.info("starting to stream twitter results ")

  context.watch(peer)

  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(SendReady)

  val buffer = collection.mutable.Queue[String]()

  implicit val ec = this.context.system.dispatcher

  override def preStart() = {
    pubsub ! Subscribe(context.self)
  }

  override def postStop() = {
    pubsub ! Unsubscribe(context.self)
  }

  def receive = {
    case SendReady => {
      while (!buffer.isEmpty) {
        val newChunk = MessageChunk(formatSSE(buffer.dequeue))
        peer ! newChunk.withAck(SendReady)
      }

    }

    case msg: String => {
      buffer.enqueue(msg)
    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      self ! PoisonPill
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class TwitterQueryStreamer(peer: ActorRef, filepath: String, qtype: String, EventStreamType: MediaType) extends Actor with ActorLogging {

  log.info("starting to stream twitter queries ")

  lazy val fs = scala.io.Source.fromFile(filepath).getLines

  context.watch(peer)

  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(SendReady)

  implicit val ec = this.context.system.dispatcher

  def receive = {
    case SendReady => {

      if (fs.hasNext) {
        qtype match {
          case "range" => {
            val qline = fs.next.split(",")
            val resp = JsObject("type" -> JsString("query"), "qtype" -> JsString("rect"), "qid" -> JsNumber(qline(0)), "north" -> JsNumber(qline(2)), "east" -> JsNumber(qline(1)), "south" -> JsNumber(qline(4)), "west" -> JsNumber(qline(3)))
            val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString))
            peer ! newChunk.withAck(SendReady)

          }
          case "knn" => {

            val qline = fs.next.split(",")
            val resp = JsObject("type" -> JsString("query"), "qtype" -> JsString("knn"), "qid" -> JsNumber(qline(0)), "lat" -> JsNumber(qline(1)), "lng" -> JsNumber(qline(2)), "k" -> JsNumber(qline(3)))
            val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString))
            peer ! newChunk.withAck(SendReady)

          }

        }

      } else {
        val newChunk = MessageChunk(Helper.createSSE("ping", JsObject("type" -> JsString("stream-end")).toString))
        peer ! newChunk
        peer ! ChunkedMessageEnd()
        self ! PoisonPill
      }
    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      self ! PoisonPill
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}

class BufferingStreamer(peer: ActorRef, pubsub: ActorRef, formatSSE: String => JsValue, EventStreamType: MediaType) extends Actor with ActorLogging {

  log.info("starting to stream string results ")

  context.watch(peer)

  val streamStart = JsObject("type" -> JsString("stream-start"))

  val responseStart = HttpResponse(entity = HttpEntity(EventStreamType.withCharset(HttpCharsets.`UTF-8`), Helper.createSSE("ping", streamStart.toString)))

  peer ! ChunkedResponseStart(responseStart).withAck(SendReady)

  val buffer = collection.mutable.Queue[String]()

  implicit val ec = this.context.system.dispatcher

  override def preStart() = {
    pubsub ! Subscribe(context.self)
  }

  override def postStop() = {
    pubsub ! Unsubscribe(context.self)
  }

  def receive = {
    case SendReady => {
      while (!buffer.isEmpty) {
        //        log.info("About to send "+buffer.size+" responses")
        val resp = JsObject("type" -> JsString("output"), "data" -> JsonParser(buffer.dequeue))
        val newChunk = MessageChunk(Helper.createSSE("ping", resp.toString))
        peer ! newChunk.withAck(SendReady)
      }

    }

    case msg: String => {
      buffer.enqueue(msg)
      if (buffer.size == 1) self ! SendReady
    }

    case Http.PeerClosed => {
      log.info("Peer terminated, ending respnse streaming ")
      self ! PoisonPill
    }

    case x: Any => {
      log.info("Receieved something unexpected: " + x.getClass.toString)
    }
  }

}