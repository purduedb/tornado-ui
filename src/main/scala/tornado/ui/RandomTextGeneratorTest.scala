package tornado.ui

import org.fluttercode.datafactory.impl.DataFactory

object TestRandomTextGen  {
  
  def main(args:Array[String]) : Unit = {
   val df = new DataFactory()
   
   for (i <- 0 to args(2).toInt) {
     println(i+ " :  " + df.getRandomWord())
   } 
    
  }
  
   
   

}