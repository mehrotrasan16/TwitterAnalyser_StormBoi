package main.java.storm;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class LossyCountBolt extends BaseRichBolt
{
	private OutputCollector myCollector;
	//private ConcurrentHashMap<String,Integer> myMapCounts;
	private ConcurrentHashMap<String,Integer> myMapCounts;
	
	private Map<String,LossyCountObject> myTweetCount = new ConcurrentHashMap<String,LossyCountObject>(); 
	private double e=0.05f;
	private int bucket_size=15;
	private int curr_bucket=1;
	private int number_of_elements=0;
	private PrintWriter myPrintWriter;	
	
	
	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.myCollector = outputCollector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("HashTag", "count","lossy_count", "bucket_id","timestamp"));		
	}

	@Override
	public void execute(Tuple inputTuple) {
		
		/*
		 *Read the data from the TweetExtraction Bolt 
		 */
		String word = inputTuple.getStringByField("hashtag");
		Integer count = (int) inputTuple.getValueByField("count");
		
		/*/
		 * Initializ variables and Test Prints 
		 */
		System.out.println(word + " ------*********------ Count: " + count.toString());
		System.out.println(inputTuple.getString(0));
		myMapCounts = new ConcurrentHashMap<String,Integer>();
		
		
		if(word != null) {
			lossy_counting(word);
		}
		myCollector.ack(inputTuple);
		
	}

	public void lossy_counting(String word) {
		 String key = word; //String key = tuple.getString(0);

	      if(!myMapCounts.containsKey(key)){
	    	  myMapCounts.put(key, 1);
	      }else{
	         Integer c = myMapCounts.get(key) + 1;
	         myMapCounts.put(key, c);
	      }
	   }
	
}
