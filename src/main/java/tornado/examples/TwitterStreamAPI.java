package tornado.examples;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwitterStreamAPI {

  public static void main(String[] args) {
    System.setProperty("twitter4j.oauth.consumerKey", "***");
    System.setProperty("twitter4j.oauth.consumerSecret","***");
    System.setProperty("twitter4j.oauth.accessToken","***");
    System.setProperty("twitter4j.oauth.accessTokenSecret","***");
    
    StatusListener listener = new StatusListener(){
      public void onStatus(Status status) {
          System.out.println(status.getUser().getName() + " : " + status.getText());
      }
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
      public void onException(Exception ex) {
          ex.printStackTrace();
      }
      @Override
      public void onScrubGeo(long arg0, long arg1) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void onStallWarning(StallWarning arg0) {
        // TODO Auto-generated method stub
        
      }
  };
  TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
  twitterStream.addListener(listener);
  // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
  twitterStream.sample();

  }

}
