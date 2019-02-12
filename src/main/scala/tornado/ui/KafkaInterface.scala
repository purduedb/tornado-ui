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

import utils._
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.collection.mutable.PriorityQueue
import spray.json.JsObject
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values
import backtype.storm.tuple.Fields
import spray.json.JsonParser
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import kafka.consumer.ConsumerIterator
import kafka.javaapi.consumer.ConsumerConnector
import spray.json.JsString

object KafkaInterface {

}

object KafkaQueryProducer {
  val config = Helper.getConfig
  val serverList = config.getString("kafka.producer.broker-list")
  val zk = config.getString("kafka.zk")
  val topic = config.getString("kafka.producer.topic")
  val producer = KafkaUtils.createStringKafkaProducer(serverList)
  
  
}

object KafkaQueryOutputConsumer {
  val config = Helper.getConfig
  val zk = config.getString("kafka.zk")
  val cgroup = config.getString("kafka.consumer.cgroup")
  val topic = config.getString("kafka.consumer.topic")
  
  val consumer = KafkaUtils.createHLKafkaConsumer(zk,cgroup)
  
  val records = KafkaUtils.createStringKafkaStream(topic, consumer).iterator()
  
//  def startConsuming(outputState: Map[String,PriorityQueue[OuputEntry]]) = {
//      
//      // this is blocking 
//      while (records.hasNext()){
//          val rec = records.next()
//          consumer.commitOffsets()
//      }  
//          
//  }
}
