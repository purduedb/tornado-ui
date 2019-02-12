package tornado.ui

class QueryProcessor {

}


abstract class Operator{
//  def name:String
  def evaluate():List[String]
  def getNext():String
}

case class Scan(ds:DataSource) extends Operator {
 val iter = ds.getIterator
 def getNext() = {
   if (iter.hasNext) iter.next()
   else null
 }
 
 def evaluate() = { 
   iter.toList
 }
 
}


case class Filter(input:Operator, ffunc: String => Boolean) extends Operator {
  def getNext() = {
    var tup = input.getNext()
    while (tup != null && !ffunc(tup)) {
      tup = input.getNext()
    }
    tup
  }
  
  def evaluate() = { // should never be called
   input.evaluate().filter { ffunc(_) }
 }
}

case class KNNSelect(input:Operator, k:Int, distFunc: String => (Double,String)) extends Operator {
  def getNext() = null // should never be called
  def evaluate() = {
    input.evaluate().map(distFunc(_)).sortWith((a,b) =>{
      a._1 < b._1
    }).take(k).map(_._2)
  }
  
}