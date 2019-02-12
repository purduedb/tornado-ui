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

import scala.io.Source
import java.io.File

class DataSources {

}

abstract class DBIterator(val inputs: Array[DBIterator]) {
  def init()
  def getNext() : Any
  def close()
}

abstract class DataSource{
  def id:String
  def getIterator:Iterator[String]  
}

case class LocalFileDataSource(val id:String, val localFilePath:String) extends DataSource {
  def getIterator:Iterator[String] = Source.fromFile(localFilePath).getLines()
}
case class LocalStreamerDataSource(val id:String, val localFilePath:String, val rate:Int){
  var currentRate = rate
  var currentIter = 0;
  
  var fileSource = Source.fromFile(new File(localFilePath))
  
  def setRate(newRate:Int) = {currentRate = newRate }
  
  def getIteratore:Iterator[String] = {
    val res = fileSource.getLines().drop(currentIter).take(currentRate)
    currentIter = currentIter + currentRate
    res
  }
  
}
//case class LocalStreamDataSource(val id:String) extends DataSource
//case class LocalFileDataSource(val id:String) extends DataSource
