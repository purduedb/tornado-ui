package utils

import akka.actor.ActorLogging
import akka.actor.Actor

object AdaptiveIndexUpdateConsumer {

}


case class UpdateHotspot(n:Double,e:Double,s:Double,w:Double)


class AdaptiveIndexUpdateConsumer extends Actor with ActorLogging {
  
  
  def receive = {
    
    case "getupdate" => {
      
    }
    
    case UpdateHotspot(n,e,s,w) => {
      
    }
    
    case _ => log.info("got uknown message")
  }
}