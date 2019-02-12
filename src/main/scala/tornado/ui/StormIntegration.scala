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

import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.task.TopologyContext
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.topology.base.BaseBasicBolt
import kafka.consumer.ConsumerIterator
import backtype.storm.topology.BasicOutputCollector
import spray.json.JsObject
import backtype.storm.tuple.Tuple
import backtype.storm.topology.base.BaseRichSpout
import kafka.javaapi.consumer.ConsumerConnector
import utils.KafkaUtils
import utils.Helper
import spray.json.JsString
import spray.json.JsonParser
import backtype.storm.tuple.Values
import backtype.storm.tuple.Fields
import org.apache.kafka.clients.producer.KafkaProducer
import spray.json.JsNumber
import org.apache.kafka.clients.producer.ProducerRecord

object StormIntegration {

}

class KafkaBolt extends BaseBasicBolt {
  var taskid: Int = 0
  var _producer: KafkaProducer[String,String] = null
  
  override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext): Unit = {
    taskid = context.getThisTaskId
    _producer =  KafkaUtils.createStringKafkaProducer(Helper.getConfig().getString("kafka.producer.broker-list"))
  }

  override def execute(tuple: Tuple, collector: BasicOutputCollector): Unit = {
    val qname: String = tuple.getString(0)
    println("got qname = " + qname)
    try {
      val qjson: JsObject = tuple.getValue(1).asInstanceOf[JsObject]
//      println(qjson.prettyPrint)
      qjson.fields.get("currentView") match {
        case Some(cv) => {
          val bounds = cv.asJsObject.getFields("north", "west", "south", "east").map { _.asInstanceOf[JsNumber].value }
          val rlng = bounds(1).toDouble + (scala.math.abs(bounds(3).toDouble - bounds(1).toDouble) * scala.util.Random.nextDouble())
          val rlat = bounds(2).toDouble + (scala.math.abs(bounds(0).toDouble - bounds(2).toDouble) * scala.util.Random.nextDouble())
          
          val resp = JsObject("qname" -> JsString(qname), 
                              "point" -> JsObject("lat" -> JsNumber(rlat), "lng" -> JsNumber(rlng)),
                              "text" -> JsString(new String(scala.util.Random.alphanumeric.take(50).toArray)))    
                              
          val kmsg = new ProducerRecord[String,String](Helper.getConfig().getString("kafka.consumer.topic"),qname,resp.toString())
          _producer.send(kmsg)
        }
        case None => {}
      }
    } catch {
      case _ => {}
    }

  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    //    declarer.declare(new Fields("word", "count"));
  }
}

class KafkaSpout(val topic: String) extends BaseRichSpout {

  var _collector: SpoutOutputCollector = null

  var _consumer: ConsumerConnector = null
  var _records: ConsumerIterator[String, String] = null

  override def open(conf: java.util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    _collector = collector
    _consumer = KafkaUtils.createHLKafkaConsumer(Helper.getConfig().getString("kafka.zk"), "queryprocExample")
    _records = KafkaUtils.createStringKafkaStream(topic, _consumer).iterator()
  }

  override def nextTuple(): Unit = {
    for (rec <- _records) {
      try {
        val qjson = JsonParser(rec.message())
        qjson.asJsObject.fields.get("name") match {
          case Some(n) => {
            val qname = n.asInstanceOf[JsString].value
            _collector.emit(new Values(qname, qjson.asJsObject))
            _consumer.commitOffsets()
          }
          case None => {}
        }
      } catch {
        case _ => {}
      }

    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("qname", "qjson"))
  }

} 