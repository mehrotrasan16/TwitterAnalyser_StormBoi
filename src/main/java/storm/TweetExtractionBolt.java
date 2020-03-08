package main.java.storm;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.storm.executor.bolt.BoltOutputCollectorImpl;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class TweetExtractionBolt extends BaseRichBolt {

	/**
	 * Private Class Members
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	/*
	 * Class Member Functions
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag","count"));
		//System.out.println("Ya boi is in the TweetExtractionBolt");
	}

	@Override
	   public void execute(Tuple tuple) {
	      Status tweet = (Status) tuple.getValueByField("Tweet");
	      for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
	         System.out.println("Hashtag: " + hashtag.getText());
	         this.collector.emit(new Values(hashtag.getText(),1));
	         //System.out.println("Emett2 emitted the stuff");
	      }
	   }

	@Override
	public void prepare(Map<String, Object> arg0, TopologyContext arg1, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

}
