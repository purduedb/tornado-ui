package utils

import spray.json.JsObject
import spray.json.JsString
import spray.json.JsNumber
import kafka.serializer.StringDecoder
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.io.BufferedSource
import scala.collection.mutable.PriorityQueue
import spray.json.JsonParser
import spray.json.JsArray

object Playground {
  
  
  def main(args :Array[String]) : Unit = {
    
    // converts CSV to JSON
       
//    val source = scala.io.Source.fromFile("data/sample_tweets.csv")
    
    
    
    ////////
    
    /// Kafka Consumer Testing
//    val zk = "localhost:2181"
//    val cgid = "g1"
//    val topic = "output"
//    
//    val conf = KafkaConsumerHelper.createConsumerConfig(zk, cgid)
//    val consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
//    val topicCountMap = Map(topic -> 1.asInstanceOf[Integer]).asJava
//
//    val consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder(), new StringDecoder())
//    val streams = consumerMap.get(topic)
//    
//    while (true) {
//      val iter = streams.get(0).iterator()
//      
////      iter.foreach(x => {
////        println("got a message: "+x.message())
////      })
//      
//      if (iter.hasNext()) {
//        val msg = iter.next.message()
//        consumer.commitOffsets()
//        println("got a message: "+msg)
//      } 
//      
//      println("====================  after foreach")
//      Thread.sleep(100)      
//    }
    
    // find max bounds
    
    val source = scala.io.Source.fromFile("data/PartitionsJSON_LargDataFile.txt")
    val arr = source.getLines().take(1).next()
    val p = JsonParser(arr).asInstanceOf[JsArray];
    var maxLat:Double = -90;
    var minLat:Double = 90;
    var maxLng:Double = -180;
    var minLng:Double = 180;
    
    p.elements.foreach { x => {
      val cmaxlat = x.asJsObject.fields.get("north").get.asInstanceOf[JsNumber].value.toDouble
      val cminlat = x.asJsObject.fields.get("south").get.asInstanceOf[JsNumber].value.toDouble
      val cmaxlng = x.asJsObject.fields.get("east").get.asInstanceOf[JsNumber].value.toDouble
      val cminlng = x.asJsObject.fields.get("west").get.asInstanceOf[JsNumber].value.toDouble
      
      if (cmaxlat > maxLat) maxLat = cmaxlat;
      if (cminlat < minLat) minLat = cminlat;
      if (cmaxlng > maxLng) maxLng = cmaxlng;
      if (cminlng < minLng) minLng = cminlat;
      
    } }
    
    println("maxLat,minLat,maxLng,minLng = "+Array(maxLat,minLat,maxLng,minLng).mkString(","))        
  }
  
//  object KNNOrdering extends Ordering[(String,String)] {
//    def compare(a:String, b:String) = {
//      val a_arr = a.split(",")
//      val b_arr = b.split(",")
//      
//    }
//  }
//  
  def generateKnnTuples(source:BufferedSource) : Unit = {
    val pq =   new PriorityQueue[String]()
    
  }
  
  def generateJoinedTuples(source:BufferedSource) : Unit = {
    
    // join tuple generation
    val buffer = scala.collection.mutable.ArrayBuffer[String]()
    val size = 10;
    val dsize = 3
    var i = 0
    
    source.getLines().foreach { x => {
      if (i > 2 && buffer.size > 2) {
        
        
      
      val arr = x.split(",")
      var arr2:Array[String] = null
      if (i % size == 0){
        buffer.drop(buffer.length / 2)       
      }
      else{
//        println(buffer)
        var in = scala.util.Random.nextInt(buffer.size)
        arr2 = buffer(in).split(",")
      }
     
      
      val qname= "q"+(scala.util.Random.nextInt(4)+1)      
          
      try {
        val res = JsObject("point1" -> JsObject("lat" -> JsNumber(arr(2)), "lng" -> JsNumber(arr(3))),
                         "point2" -> JsObject("lat" -> JsNumber(arr2(2)), "lng" -> JsNumber(arr2(3))),
                         "oid1" -> JsString(arr(0)),
                         "oid2" -> JsString(arr2(0)),
                         "text1" -> JsString(arr(5)),
                         "text2" -> JsString(arr2(5)),
                         "tag" -> JsString("+"),
                         "name" -> JsString(qname))
        println(res.toString())
      }
      catch {
        case _ => {}
      }
      }
      buffer += x
      i = i + 1
      
    } }
    
  }
}