/**
 * Copyright 2015
 * Author : Thamir Qadah 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tornado.ui

import org.scalatest._
import utils.KafkaUtils
import org.apache.kafka.clients.producer.ProducerRecord
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

import java.util.HashMap

class  KafkaInterfaceTest extends FlatSpec with Matchers{

  val topic = "queries"
  
  val producer = KafkaUtils.createStringKafkaProducer("localhost:9092")
  val consumer = KafkaUtils.createHLKafkaConsumer("localhost:2181","test")
  
  
  val message = "q" + getRandomString(10)
  
  def getRandomString(length:Int) = {
    new String(scala.util.Random.alphanumeric.take(length).toArray)
  }
  
  "A producer" should "write " + message+  " from topic("+topic+")" in {    
    val rec = new ProducerRecord[String,String](topic,"q1",message)
    val md = producer.send(rec).get
    md.topic() should be ("queries")
  }  

  "A consumer" should "read " + message+  " from topic("+topic+")" in {
    
val records = KafkaUtils.createStringKafkaStream(topic, consumer).iterator()
    records should not be (null)
    // Run in future
    val cf = Future {
      var res:String = null
      if (records.hasNext()){
          res = records.next().message()
          consumer.commitOffsets()
      }  
      res
    }
    
    val cres = Await.result(cf, 1 seconds)    
    cres should be (message)
        
  }
  
  
  

//  
//  def process(records:java.util.Map[String,ConsumerRecords[String,String]]) {
//     val processedOffsets = new HashMap[TopicPartition, Long]();
//     
//     for(recordMetadata <- records.entrySet()) {
//          val recordsPerTopic = recordMetadata.getValue().records();
//          for( i <- 0 to recordsPerTopic.size()) {
//               val record = recordsPerTopic.get(i);
//               // process record
//               println(record.value())
//               try {
//                processedOffsets.put(record.topicAndPartition(), record.offset());
//               } catch {
//               case e:Exception => e.printStackTrace()
//               }               
//          }
//     }
//     return processedOffsets; 
// }
}