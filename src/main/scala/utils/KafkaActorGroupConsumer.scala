package utils

import akka.actor.Actor
import akka.actor.ActorLogging
import kafka.consumer.KafkaStream
import scala.concurrent.Future
import akka.actor.ActorSystem
import kafka.consumer.ConsumerConfig
import java.util.Properties
import java.util.HashMap
import scala.collection.JavaConverters._
import akka.actor.Props
import kafka.serializer.StringDecoder
import akka.actor.ActorRef
import kafka.javaapi.consumer.ConsumerConnector
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

object KafkaActorGroupConsumer extends App {

  val sys = ActorSystem("kafkaConsumer")
  val printer = sys.actorOf(Props[Printer])
  
  val zk = "localhost:2182"
  val cgid = "cgid"
  val topic = "output"
    
  KafkaConsumerHelper.startKafkaConsumer(zk,cgid,topic, sys, printer)

  class Printer extends Actor with ActorLogging{
    def receive = {
      case KafkaStringMessage(x) =>
        log.info("received from kafka : "+x)
    }
  }

  
}

object KafkaConsumerHelper {
  
  def createKafkaConsumer(zk: String, cgid: String, topic: String) = {
    val conf = KafkaConsumerHelper.createConsumerConfig(zk, cgid)
    kafka.consumer.Consumer.createJavaConsumerConnector(conf);    
  }

  def startKafkaConsumer(zk: String, cgid: String, topic: String, sys: ActorSystem, target: ActorRef) = {
    val conf = KafkaConsumerHelper.createConsumerConfig(zk, cgid)
    val consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
    val topicCountMap = Map(topic -> 1.asInstanceOf[Integer]).asJava

    val consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder(), new StringDecoder())
    val streams = consumerMap.get(topic)

    val aprops = Props(classOf[KafkaStringConsumer], consumer, streams.get(0), target)
    val c = sys.actorOf(aprops)

    c ! "checkMsgs"
    c
  }

  def createConsumerConfig(a_zookeeper: String, a_groupId: String): ConsumerConfig = {
    val props = new Properties();
    props.put("zookeeper.connect", a_zookeeper);
    props.put("group.id", a_groupId);
    props.put("zookeeper.session.timeout.ms", "1000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    (new ConsumerConfig(props))
  }

}

case class SubscribeStreamer(ar:ActorRef)
case class UnsubscribeStreamer(ar:ActorRef)
object GetLastMessage

class KafkaPubSub(zk: String, cgid: String, topic: String) extends Actor with ActorLogging {
  implicit val ec = this.context.system.dispatcher
  val conf = KafkaConsumerHelper.createConsumerConfig(zk, cgid)
  val consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
  val topicCountMap = Map(topic -> 1.asInstanceOf[Integer]).asJava
  val consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder(), new StringDecoder())
  val streams = consumerMap.get(topic)
  val stream = streams.get(0)
  
  val localSubs = ArrayBuffer[ActorRef]()
  
//  val f = Future {
//      val iter = stream.iterator()
//      if (iter.hasNext()){
//        val x =  iter.next()
//        consumer.commitOffsets()        
//        localSubs.map { _ ! (new KafkaStringMessage(x.message)) }
//      }
//    }
  
  this.context.system.scheduler.scheduleOnce( Helper.getConfig().getInt("kafka.consumer.actorCheckRate") milliseconds, self, "checkMessages")
  this.context.system.scheduler.schedule( Helper.getConfig().getInt("kafka.consumer.statusUpdateRate") milliseconds,  
                          Helper.getConfig().getInt("kafka.consumer.statusUpdateRate") milliseconds, self, "printCounts")
  
  var lastMessage = ""
  
  def receive = {
      
    case "printCounts" => {
       log.info("client count = "+localSubs.size)
    }
    
    case "checkMessages" => {
      
      val iter = stream.iterator()
      if (iter.hasNext()){
        val x =  iter.next()
        consumer.commitOffsets()
        lastMessage = x.message()
//        log.info("sending out :"+lastMessage)
        localSubs.map { _ ! (new KafkaStringMessage(lastMessage)) }        
        
      }
      else {
        log.info("no messages = "+localSubs.size)
      }
      
      this.context.system.scheduler.scheduleOnce(  Helper.getConfig().getInt("kafka.consumer.actorCheckRate") milliseconds, self, "checkMessages")
    }
    
    case SubscribeStreamer(aref) => {
      log.info("subscribing  "+aref)
      localSubs += aref
    }
    
    case UnsubscribeStreamer(aref) =>{
      log.info("unsubscriBing  "+aref)
      localSubs -= aref
    }
    
    case GetLastMessage => {
      sender ! (new KafkaStringMessage(lastMessage))
    }
    
    
    case x:Any => log.info("got a message : "+ x.getClass().getName)
  }
}

case class KafkaStringMessage(val x: String) extends Serializable

class KafkaStringConsumer(val consumer:ConsumerConnector, val stream: KafkaStream[String, String], val target: ActorRef) extends Actor with ActorLogging {
  implicit val ec = this.context.system.dispatcher
  
  
  def receive = {
    case "checkMsgs" => {
      val f = Future {
        
        stream.iterator.foreach(x => {
          //          log.info("KafkaMessage " + threadNum + " : " + x.message)
          target ! (new KafkaStringMessage(x.message))
          // TODO: Need to commit offsets, validate that this works
          consumer.commitOffsets()
        })
        
      }
    }
    case _ => log.info("got a message")
  }
}