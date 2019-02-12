package utils

import akka.actor._
import akka.event._
import akka.dispatch._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent._
import scala.concurrent.duration._
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import scala.collection.mutable.ArrayBuffer
import akka.io.{ IO, Tcp }
import java.net.InetSocketAddress
import org.apache.log4j.Logger
import akka.util.ByteString
import akka.util.ByteStringBuilder
import java.nio.charset.Charset
import java.nio.charset.MalformedInputException
import java.nio.charset.CodingErrorAction

/**
 * Testing Network controllers as the following example:
 * 1. start server : $ ./target/pack/bin/StreamConroller -n path/to/datafile localhost 7888 1 1
 * 2. start client : $ nc localhost 7888
 */

object StreamController extends App {

  val log = Logger.getLogger(this.getClass().getName())

  if (args.length > 1) {
    args(0) match {
      case "-k" => {
        log.info("Kafka Based Controller")
        val filepath = this.args(1)
        val topic = args(4)
        val count = this.args(2).toInt
        val period = this.args(3).toInt

        val rate = count / period

        if (this.args.length != 5) {
          log.error("StreamController -k sourceFileName topic count period , for Streaming into kafka topic")
          System.exit(0)

        } else {

          log.info(s"Rate = $rate per $period seconds")
          implicit val system = ActorSystem("stream-controller")
          implicit val ec = system.dispatcher
          implicit val timeout = Timeout(3)

          val c = system.actorOf(Props(classOf[KafkaTopicController], filepath, count, topic), name = "controller")

          system.scheduler.schedule(1 seconds, period seconds) {
            c ! "sendout"
          }

        }
      }
      case "-n" => {
        log.info("Network Socket Based Controller")

        val filepath = this.args(1)
        val host = args(2)
        val port = args(3).toInt
        val count = this.args(4).toInt
        val period = this.args(5).toInt

        val rate = count / period

        if (this.args.length != 6) {
          log.error("StreamController -n sourceFileName host port count period , for Streaming from a network socket")
          System.exit(0)

        } else {

          log.info(s"Rate = $rate per $period seconds")
          implicit val system = ActorSystem("stream-controller")
          implicit val ec = system.dispatcher
          implicit val timeout = Timeout(3)

          val c = system.actorOf(Props(classOf[NetworkSocketControllerServer], filepath, host, port, count, period), name = "controller")

        }

      }

    }
  } else {
    log.info("""Usage: 
        | StreamController -k sourceFileName topic count period , for Streaming into kafka topic
        | StreamController -n sourceFileName host port count period , for Streaming from a network socket   			
    """)
  }

}

class NetworkSocketControllerServer(filepath: String, host: String, port: Int, icount: Int, period: Int) extends Actor with ActorLogging {

  import Tcp._
  import context.system

  IO(Tcp) ! Bind(self, new InetSocketAddress(host, port))
  var count = icount
  
  def receive = {
    case b @ Bound(localAddress) => {
      log.info("Bound to : " + localAddress.toString())
    }

    case CommandFailed(_: Bind) => { context stop self }

    case c @ Connected(remote, local) =>
      {
//        implicit val codec = scala.io.Codec("UTF-8")
//        codec.onMalformedInput(CodingErrorAction.REPLACE)
//        codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
        
//        val fs = scala.io.Source.fromFile(filepath).getLines
        val connection = sender
        val handler = context.actorOf(Props(classOf[SimplisticHandler], filepath, count, period, connection))        
        connection ! Register(handler)
        log.info("Connected to client at : " + remote.toString())
      }
    case cu:ChangeCount =>  {context.children.foreach( _ ! cu)}
    case _ => log.info("got something")
  }

}

object RemoveMe
case class ChangeCount(newCount:Int)

class SimplisticHandler(fp:String, icount: Int, period: Int, remote: ActorRef) extends Actor with ActorLogging {

  //  lazy val fs = scala.io.Source.fromFile(filepath).getLines
  //  fs.next // skipping hte first line
  import Tcp._

  implicit val ec = context.system.dispatcher
  implicit val timeout = Timeout(3)
  
  implicit val codec = scala.io.Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.IGNORE)
  codec.onUnmappableCharacter(CodingErrorAction.IGNORE)
  
  var cfs = scala.io.Source.fromFile(fp).getLines
  var iterationCount:Long = 1
  var tupleSentCount:Int = 0
  var tobeSent:Int = 0
  
  var count = icount
  
  val s = context.system.scheduler.schedule(0 seconds, period seconds) {
    self ! "sendout"
  }
  
  def sendOutData(ccount:Int) = {
    val bsb = new ByteStringBuilder()      
      while (cfs.hasNext && tupleSentCount < ccount) {
        bsb.putBytes((cfs.next + "\n").getBytes())
        remote ! Write(bsb.result)
        bsb.clear()
        tupleSentCount = tupleSentCount + 1
      }
  }

  def receive = {
    case Received(data) => { sender ! Write(ByteString("Server: You should not send anything to me. Please don't do it again.\n")) }
    case ChangeCount(nc) => count = nc
    case PeerClosed => {
      log.info("Client Teminated, we have done "+iterationCount+" iterations over input file")
      s.cancel      
      context stop self
    }

    case "sendout" => {
      //log.info("sending out")
      sendOutData(count)
      
      if (tupleSentCount != count){
        //Disconnet on EOF
//        log.info("EOF reached, Disconneting client")   
//        s.cancel
//        context stop self

        // Repeat on EOF
        log.info("EOF reached, Reopening the file and starting from the begining")
        // file data finished before count reached
        // reopen the file and send the rest
        iterationCount = iterationCount +1
        cfs = scala.io.Source.fromFile(fp).getLines        
        val tobeSent = count - tupleSentCount
        sendOutData(tobeSent)
        tupleSentCount = 0
        
      }
      else {
        tupleSentCount = 0
      }
    }
  }
}

class KafkaTopicController(filepath: String, count: Int, topic: String) extends Actor with ActorLogging {

  lazy val fs = scala.io.Source.fromFile(filepath).getLines.toArray[String]

  val props = new java.util.Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  //props.put("partitioner.class", "example.producer.SimplePartitioner")
  props.put("request.required.acks", "1")

  val pconfig = new ProducerConfig(props)
  val producer = new Producer[String, String](pconfig)

  var marker = 1 // skipping hte first line

  def receive = {
    case "sendout" => {
      //      log.info("sending out")

      val res = fs(marker)

      for (i <- 1 to count) {
        val data = new KeyedMessage[String, String](topic, res)
        producer.send(data)

        //        log.info(data.toString)
        marker = marker + 1
      }
    }
    case _ => log.info("got something")
  }

}

object QueryStreamController extends App {

  //  this.args.foreach(s => Console.println(s))
  val filepath = this.args(0)
  val topic = args(3)
  val count = this.args(1).toInt
  val period = this.args(2).toInt

  val rate = count / period

  if (this.args.length != 4) {
    Console.println("Incorrect number of parameters: filePath count period topic")

  } else {

    println(s"Rate = $rate per $period seconds")
    implicit val system = ActorSystem("stream-controller")
    implicit val ec = system.dispatcher
    implicit val timeout = Timeout(3)

    val c = system.actorOf(Props(classOf[BerlinMODQueryController], filepath, count, topic), name = "controller")

    system.scheduler.scheduleOnce(1 seconds) { c ! "sendout" }
  }

}

