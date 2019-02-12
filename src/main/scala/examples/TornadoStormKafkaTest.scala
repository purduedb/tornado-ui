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

package examples

import backtype.storm.task._
import backtype.storm.topology._
import backtype.storm.tuple._
import backtype.storm.topology.base._
import backtype.storm.Config
import backtype.storm.LocalCluster
import scala.collection.mutable.HashMap
import tornado.ui.KafkaBolt
import tornado.ui.KafkaSpout

object TornadoStormKafkaTest {
  
  def main(args:Array[String]) : Unit ={
    val builder = new TopologyBuilder()
    builder.setSpout("kafkaspout", new KafkaSpout("queries"), 1);
    builder.setBolt("kafkabolt", new KafkaBolt(), 1).shuffleGrouping("kafkaspout");
    
    val conf = new Config();
    conf.setDebug(true);
    
//    conf.setMaxTaskParallelism(3);

    val cluster :LocalCluster= new LocalCluster();
    cluster.submitTopology("TornadoLocalClusterTest", conf, builder.createTopology());

//    Thread.sleep(1000000);

//    cluster.shutdown();
    
  }

}
