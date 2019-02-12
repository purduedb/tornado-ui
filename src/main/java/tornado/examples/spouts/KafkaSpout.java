package tornado.examples.spouts;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;


public class KafkaSpout extends BaseRichSpout {
  private String topic;
  SpoutOutputCollector _collector;
  
  public KafkaSpout(String topic) {
    this.topic = topic;
  }

  @Override
  public void nextTuple() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void open(Map map, TopologyContext tctx, SpoutOutputCollector collector) {
    _collector = collector;
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("query"));
    
  }
  
}
