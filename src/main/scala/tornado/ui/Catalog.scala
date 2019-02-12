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

import scala.collection._
import spray.json.JsObject
import spray.json.JsArray
import spray.json.JsString
import scala.collection.mutable.PriorityQueue
import utils.Unsubscribe
import scala.concurrent.Await
import scala.collection.mutable.ArrayBuffer
import akka.actor.Terminated
import akka.actor.ActorLogging
import utils.Forward
import akka.actor.Actor
import utils.Subscribe
import akka.actor.ActorRef
import utils.Helper
import utils.KafkaUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.concurrent.atomic.AtomicLong

case class OutputEntry(val ts: Long, val entry: JsObject)

object Catalog {
  
  // Synchronized access -- Performance hit 
  val json_cqueries = new mutable.LinkedHashMap[String, JsObject]() with scala.collection.mutable.SynchronizedMap[String, JsObject]
  val json_squeries = new mutable.LinkedHashMap[String, JsObject]() with scala.collection.mutable.SynchronizedMap[String, JsObject]

  val datasources = mutable.Map[String, DataSource]()

  val queryState = mutable.Map[String, ActorRef]()
  
  var kafkaConsumerCount:AtomicLong = new AtomicLong(0)
  
  var producer : KafkaProducer[String,String] = null
  
  if (Helper.getConfig().getBoolean("kafka.enabled")) {
    producer = KafkaUtils.createStringKafkaProducer(Helper.getConfig().getString("kafka.producer.broker-list"))   
  }
  
  def KafkaProducerSend(rec: ProducerRecord[String,String]) : Unit = {
    if (Helper.getConfig().getBoolean("kafka.enabled")) {
      producer.send(rec)
    }
  }
  
  def getRegisteredCQueries: JsArray = {    
    JsArray(json_cqueries.map { q => {
      val qname = q._1
      val qjso = q._2
      val color = getStringValue("outputColor", qjso)
      val sqltext = getStringValue("sqltext", qjso)
      JsObject("name" -> JsString(qname), "outputColor" -> JsString(color), "sqltext" -> JsString(sqltext))
    } }.toVector)
  }
  
  def getStringValue(key:String, jso:JsObject) : String = {
    jso.fields.get(key).get.asInstanceOf[JsString].value
  }

  def getRegisteredSQueries: JsArray = {
    JsArray(json_squeries.keys.map { JsString(_) }.toVector)
  }
  
//  def registerCQuery(qname:String,qjson:JsObject, actorSystem:) = {
//    
//    
//  }

}

case class QueryStateEntry(val json:JsObject, val outputPubSubActor:ActorRef) 
case class RegisterQuery(qname:String,qjson:JsObject)
case class UnRegisterQuery(qname:String)
case class UpdateQuery(qname:String,qjson:JsObject)

case class PurgeQueryOutputState(qname:String)
object PurgeOutputState

case class InsertOutputEntry(oe:OutputEntry)

class LocalPubSubActor[T] extends Actor with ActorLogging {

  val localSubs = ArrayBuffer[ActorRef]()

  def receive = {
    case Subscribe(ap) => {

      ap match {
        case x: ActorRef => {
          log.info("Subscribing Local actor: " + ap)
          this.context.watch(x)
          localSubs += x

        }
        case _ => {
          log.info("unknown subscribe message: " + ap)
        }
      }

    }
    case Unsubscribe(ap) => {

      ap match {

        case x: ActorRef => {
          log.info("UnSubscribing local: " + ap)
          localSubs -= x
        }
        case _ => {
          log.info("unknown unsubscribe message: " + ap)
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

    }

    case Terminated(t) => {

      // Works for local subscribers 
      // If local subscribers are terminated without sending an UnSubscribe message
      t match {
        case x: ActorRef => {
          localSubs -= x
        }
      }

      log.info("Ther actor : " + t + " is terminated")
    }

    case msg => {
      log.error("received a message of type : " + msg.getClass.getName())
    }
  }
}