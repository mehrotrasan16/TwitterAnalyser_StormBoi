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

public class LossyCountBolt extends BaseRichBolt {
	private OutputCollector myCollector;
	// private ConcurrentHashMap<String,Integer> myMapCounts;
	private ConcurrentHashMap<String, Integer> myMapCounts = new ConcurrentHashMap<String, Integer>();
	private Map<String, LossyCountObject> myTweetCount = new ConcurrentHashMap<String, LossyCountObject>();

	private double e = 0.05f;
	private int bucket_size = 15; // w
	private int curr_bucket = 1;
	private int number_of_elements = 0; // N
	private PrintWriter myPrintWriter;

	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.myCollector = outputCollector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("HashTag", "count","lossy_count",
		// "bucket_id","timestamp"));
	}

	@Override
	public void execute(Tuple inputTuple) {

		/*
		 * Read the data from the TweetExtraction Bolt
		 */
		String word = inputTuple.getStringByField("hashtag");
		Integer count = (int) inputTuple.getValueByField("count");

		/*
		 * / Initialize variables and Test Prints
		 */
		System.out.println(word + " ------*********------ Count: " + myMapCounts.get(word));
		System.out.println(inputTuple.getString(0));

		if (word != null) {
			plain_counting(word);
		}
		myCollector.ack(inputTuple);

	}

	public void plain_counting(String word) {
		String key = word; // String key = tuple.getString(0);

		if (!myMapCounts.containsKey(key)) {
			myMapCounts.put(key, 1);
		} else {
			Integer c = myMapCounts.get(key) + 1;
			myMapCounts.put(key, c);
		}
	}

	public void lossy_counting(String word) {
		String key = word;
		System.out.println("called lossy_counting");

		if (number_of_elements < bucket_size) { // or N mod w == 0

			/*
			 *Plain count logic 
			 */			
			if (!myTweetCount.containsKey(key)) {
				LossyCountObject newKey = new LossyCountObject();
				newKey.element = key;
				newKey.freq = 1;
				newKey.delta = curr_bucket - 1;
				newKey.timestamp = System.currentTimeMillis();
				myTweetCount.put(key, newKey);
				number_of_elements++;
			} else {
				LossyCountObject existingKey = myTweetCount.get(key);
				existingKey.freq = existingKey.freq + 1;
				myTweetCount.put(key, existingKey);
			}
		}
		if (number_of_elements >= bucket_size) {
			/*
			 * Delete ELements
			 */
			prune();
			/*
			 * Increment bucket number
			 */
			curr_bucket++;
		}

	}

	public void prune() {
		System.out.println("Called Prune");
		myTweetCount.forEach((String key, LossyCountObject value) -> {
			System.out.println("key: " + key + "\tvalue: " + value);
//			if(value.freq + value.delta <= curr_bucket) {
//				myTweetCount.remove(key);
//			}
		});
	}
}
