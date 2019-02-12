
package examples

import java.util.concurrent.LinkedBlockingQueue
import com.twitter.hbc.core._
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.endpoint.Location
import com.twitter.hbc.core.endpoint.Location.Coordinate
import com.twitter.hbc.httpclient.auth.OAuth1
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.ArrayList

object TwitterHBCExample {
  
  def main(args:Array[String]) = {
    run(args(0),args(1),args(2),args(3))
  }
  
  def run(consumerKey:String, consumerSecret:String, token:String, secret:String) : Unit = {
    val bqueue = new LinkedBlockingQueue[String](10000)
    val endpoint = new StatusesFilterEndpoint()
    // New York -74,40,-73,41
    val sw = new Coordinate(-74D,40D)
    val ne = new Coordinate(-73D,41D)
    val ny_loc = new Location(sw,ne)
    
    
    val locs = new ArrayList[Location]();
    
    locs.add(ny_loc)
    endpoint.locations(locs)
    
//    val terms = new ArrayList[String]();
    
//    terms.add("Purdue");
//    endpoint.trackTerms(terms)
    
    val auth:Authentication = new OAuth1(consumerKey, consumerSecret, token, secret);
    val client  = new ClientBuilder()
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(bqueue))
            .build();

    // Establish a connection
    println("Connecting to twitter ...")
    client.connect();
    println("Connected.")
    // Do whatever needs to be done with messages
    var msgRead = 0
    println("reading 10 messages")
    for (msgRead <- 0 to 10) {
      val msg = bqueue.take();
      println(msg);
    }
    
    println("reading another 10 messages")
    for (msgRead <- 0 to 10) {
      val msg = bqueue.take();
      println(msg);
    }
    
    println("stoping client now ...")
    client.stop();
    
  }

}