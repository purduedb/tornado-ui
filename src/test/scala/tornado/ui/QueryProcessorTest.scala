package tornado.ui

/**
 * @author qadahtm
 */

import org.scalatest._
import java.util.ArrayList


class QueryProcessorTest extends FlatSpec with Matchers {
  
  val dummy = new DummyDataSource()
  
  val dummy2 = new DummyDataSource2()
  
  "A scan operator " should " scan tuple " in {
    val scan = new Scan(dummy)  
    scan.getNext() should be ("test1")
    scan.getNext() should be ("test2")
    scan.getNext() should be ("test3")
    scan.getNext() should be (null)
  }
  
  "A filter operator " should "filter input" in {
    val scan = new Scan(dummy)  
    val filter = new Filter(scan, _ == "test2")
    filter.getNext() should be ("test2")
    
  }
  
  "A KNNSelect operator" should "return 4-nearest-neighbor" in {
    val scan = new Scan(dummy2)  
    val knn = new KNNSelect(scan, 4, x => {
      val v = x.toInt
      val f =6
      val d = (v - f).toDouble.abs
//      println(v +" is of distance "+ d + " from "+f)
      (d, x)
    })
    
    knn.evaluate() should be (Array(5,4,8,3).map(_.toString))    
  }
  
  "A KNNSelect operator" should "return 3-nearest-neighbor" in {
    val scan = new Scan(dummy2)  
    val knn = new KNNSelect(scan, 3, x => {
      val v = x.toInt
      val f = 10
      val d = (v - f).toDouble.abs
      (d, x)
    })
    knn.evaluate() should be (Array(11,8,13).map(_.toString))
  }
  
}

class DummyDataSource2 extends DataSource {
  import scala.util.Random  
  val dummyArray = Array(1,2,3,4,5,42,23,11,34,44,2,67,8,55,24,43,13,54).map { _.toString() }
  def id = "dummy2"
  def getIterator:Iterator[String] = dummyArray.iterator
}

class DummyDataSource extends DataSource{
  val dummyArray = Array("test1","test2","test3")
  def id = "dummy"
  def getIterator:Iterator[String] = dummyArray.iterator
}