package utils

import scala.collection.mutable.ArrayBuffer
import akka.actor.ActorLogging
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import kafka.javaapi.producer.Producer
import akka.actor.Actor
import akka.actor.PoisonPill
import scala.concurrent.duration._
import akka.actor.ActorPath
import scala.reflect.ClassTag
import org.apache.spark.streaming.receiver.ActorHelper
import akka.actor.ActorRef
import scala.concurrent.Await
import akka.actor.Terminated
import akka.io.Tcp
import akka.io.IO
import java.net.InetSocketAddress
import java.nio.charset.CodingErrorAction
import akka.util.ByteString
import akka.util.ByteStringBuilder
import akka.actor.Props

case class Subscribe(ap: AnyRef)
case class Unsubscribe(ap: AnyRef)

case class Forward[T](val msg: T)

class SparkSampleActorReceiver[T: ClassTag](urlOfPublisher: String)
  extends Actor with ActorHelper {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart = remotePublisher ! Subscribe(context.self)

  def receive = {
    case msg => {
      store(msg.asInstanceOf[T])
    }
  }

  override def postStop = remotePublisher ! Unsubscribe(context.self)

  
}

class PubSubActor[T] extends Actor with ActorLogging {

  val localSubs = ArrayBuffer[ActorRef]()
  val remoteSubs = ArrayBuffer[String]()

  def receive = {
    case Subscribe(ap) => {
      
      ap match {
        case x: String => {
          log.info("Subscribing Remote actor: " + ap)
          val timeout = 100 seconds

          val f = context.actorSelection(x).resolveOne(timeout).mapTo[ActorRef]
          val aref = Await.result(f, timeout)
          this.context.watch(aref)

          remoteSubs += x

        }
        case x: ActorRef => {
          log.info("Subscribing Local actor: " + ap)
          this.context.watch(x)
          localSubs += x

        }
      }

    }
    case Unsubscribe(ap) => {
      
      //      subscribers.dropWhile(_.eq(ap))
      ap match {
        case x: String => {
          log.info("UnSubscribing Remote: " + ap)
          remoteSubs -= x
        }
        case x: ActorRef => {
          log.info("UnSubscribing local: " + ap)
          localSubs -= x
        }
      }

    }
    case x: Forward[T] => {
      //      log.info("Forwarding a message : "+x.msg)      
      if (localSubs.size > 0) {
        localSubs.foreach(s => {
          s ! x.msg
        })
      }

      if (remoteSubs.size > 0) {
        //TODO : resolve remote reference first
        remoteSubs.foreach(s => {
          this.context.actorSelection(s) ! x.msg
        })
      }
    }

    case Terminated(t) => {
      
      // Works for local subscribers 
      // If local subscribers are terminated without sending an UnSubscribe message
      t match {
        case x:ActorRef =>{
        	localSubs -= x
        }
      }
      
      
      
      log.info("Ther actor : "+t+" is terminated")
    }
    
    case msg => {
      log.error("received a message of type : "+msg.getClass.getName())
    }
  }
}

class NetworkPubSubHandler(remote: ActorRef) extends Actor with ActorLogging {


  import Tcp._
  
  def receive = {
    case Received(data) => {       
      remote ! Write(data) 
     }
    case PeerClosed => {
      log.info("Client Teminated")
      context stop self
    }

  }
}

case class TopicSubscribe(topic:String, ref:AnyRef)
case class TopicUnsubscribe(topic:String, ref:AnyRef)
case class TopicForward[T](topic:String, msg: T)
case class CreateTopic(topicString:String)
case class DeleteTopic(topicString:String)

class TopicPubSubActor[T](host: String, port: Int) extends Actor with ActorLogging {

  val defaultTopic = "defaultTopic"
    
  val localSubs = scala.collection.mutable.Map[String,ArrayBuffer[ActorRef]]()
  localSubs += (defaultTopic -> ArrayBuffer[ActorRef]())
  val remoteSubs = scala.collection.mutable.Map[String,ArrayBuffer[ActorRef]]()
  remoteSubs += (defaultTopic -> ArrayBuffer[ActorRef]())
  
  import Tcp._
  import context.system

  IO(Tcp) ! Bind(self, new InetSocketAddress(host, port))
  
  def receive = {
    
    case b @ Bound(localAddress) => {
      log.info("Bound to : " + localAddress.toString())
    }

    case CommandFailed(_: Bind) => { context stop self }

    case c @ Connected(remote, local) =>
      {
        
        val connection = sender
        val handler = context.actorOf(Props(classOf[NetworkPubSubHandler],connection))
        connection ! Register(handler)
        log.info("Connected to client at : " + remote.toString())
      }
    
    case CreateTopic(t) => {
      localSubs += (t -> ArrayBuffer[ActorRef]())
      remoteSubs += (t -> ArrayBuffer[ActorRef]())
    }
    
    case DeleteTopic(t) => {
      localSubs -= t
      remoteSubs -= t
    }
    case TopicSubscribe(topic,ap) => {
      
      ap match {
        case x: String => {
          log.info(s"Subscribing Remote actor to topic ($topic): " + ap)
          val timeout = 100 seconds
          
          if (remoteSubs.keySet.contains(topic)){
          val f = context.actorSelection(x).resolveOne(timeout).mapTo[ActorRef]
          val aref = Await.result(f, timeout)
          this.context.watch(aref)
            remoteSubs(topic) += aref
          }
          else{
            log.warning(sender.toString+s" is trying to subscribe to nonexistent topic ($topic)")
          }
          
        }
        case x: ActorRef => {
          log.info(s"Subscribing Local actor to topic ($topic): " + ap)
          
          if (localSubs.keySet.contains(topic)){
            localSubs(topic) += x
            this.context.watch(x)
          }
          else{
            log.warning(sender.toString+s" is trying to subscribe to nonexistent topic ($topic)")
          }
          
        }
      }

    }
    case TopicUnsubscribe(topic,ap) => {
      
      //      subscribers.dropWhile(_.eq(ap))
      ap match {
        case x: String => {
          log.info("Unsubscribing Remote from topic ($topic): " + ap)
          if (remoteSubs.keySet.contains(topic)){
            val timeout = 100 seconds
            
            val f = context.actorSelection(x).resolveOne(timeout).mapTo[ActorRef]
            val aref = Await.result(f, timeout)
        	  remoteSubs(topic) -= aref
          }
          else{
            log.warning(sender.toString+s" is trying to unsubscribe from nonexistent topic ($topic)")
          }
          
        }
        case x: ActorRef => {
          log.info("Unsubscribing Local from topic ($topic): " + ap)
          
          if (localSubs.keySet.contains(topic)){
            localSubs(topic) -= x
            this.context.unwatch(x)            
          }
          else{
            log.warning(sender.toString+s" is trying to unsubscribe from nonexistent topic ($topic)")
          }                   
        }
      }

    }
    
    
    case x: TopicForward[T] => {
      //      log.info("Forwarding a message : "+x.msg)  
      if (localSubs.keySet.contains(x.topic)){
    	  if (localSubs(x.topic).size > 0) {
    		  localSubs(x.topic).foreach(s => {
	          s ! x.msg
	        })
	      }
      }
      
      
      if (remoteSubs.keySet.contains(x.topic)){
    	  if (remoteSubs.size > 0) {
	        //TODO : resolve remote reference first
	        remoteSubs(x.topic).foreach(s => {
	          s ! x.msg
	        })
	      }
      }

    }
    
    case Subscribe(ap) => {
      
      ap match {
        case x: String => {
          log.info("Subscribing Remote actor: " + ap)
          val timeout = 100 seconds

          val f = context.actorSelection(x).resolveOne(timeout).mapTo[ActorRef]
          val aref = Await.result(f, timeout)
          this.context.watch(aref)

          remoteSubs(defaultTopic) += aref

        }
        case x: ActorRef => {
          log.info("Subscribing Local actor: " + ap)
          this.context.watch(x)
          localSubs(defaultTopic) += x

        }
      }

    }
    case Unsubscribe(ap) => {
      
      //      subscribers.dropWhile(_.eq(ap))
      ap match {
        case x: ActorRef => {
          log.info("UnSubscribing local: " + ap)
          localSubs(defaultTopic) -= x
        }
      }

    }
    case x: Forward[T] => {
      //      log.info("Forwarding a message : "+x.msg)      
      if (localSubs(defaultTopic).size > 0) {
        localSubs(defaultTopic).foreach(s => {
          s ! x.msg
        })
      }

      if (remoteSubs(defaultTopic).size > 0) {
        //TODO : resolve remote reference first
        remoteSubs(defaultTopic).foreach(s => {
//          this.context.actorSelection(s) ! x.msg
          s ! x.msg
        })
      }
    }

    case Terminated(t) => {
      
      // Works for local subscribers 
      // If local subscribers are terminated without sending an UnSubscribe message
      // remove them from all sub lists
      localSubs.values.foreach(sub => {
        sub -= t
      })
      
      log.info("Ther actor : "+t+" is terminated and removed from all subs")
    }
    
    case msg => {
      log.error("received a message of type : "+msg.getClass.getName())
    }
  }
}

class PrinterActor extends Actor with ActorLogging {

  log.info("Printer start")

  def receive = {
    case x: Any => {
      log.info(x.toString)
    }
  }
}

class BerlinMODQueryController(filepath: String, count: Int, topic: String) extends Actor with ActorLogging {

  implicit val ec = context.system.dispatcher

  lazy val fs = scala.io.Source.fromFile(filepath).getLines

  val props = new java.util.Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  //props.put("partitioner.class", "example.producer.SimplePartitioner")
  props.put("request.required.acks", "1")

  val pconfig = new ProducerConfig(props)
  val producer = new Producer[String, String](pconfig)

  var marker = 1
  // skipping the first line
  fs.next()
  var qcount = 1
  val maxcount = 10

  def receive = {
    case "sendout" => {
      //      log.info("sending out")
      val queries = ArrayBuffer[String]()

      if (fs.hasNext) {
        for (i <- 1 to count) {
          val q = Helper.getSimpleRangeQuery(fs, marker);
          val res = q._2._1 + "," + q._2._2.mkString(",")

          val data = new KeyedMessage[String, String](topic, res)
          producer.send(data)

          queries += res
          //        log.info(res.toString)
          marker = q._1 + 1
        }
        qcount = qcount + 1
        context.system.scheduler.scheduleOnce(1 seconds) { self ! "sendout" }
      } else {
        log.info("done and not sending anymore")
        self ! PoisonPill
      }
    }
    case _ => log.info("got something")
  }

}