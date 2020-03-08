package main.java.storm;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.joda.time.DateTime;

import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class TweetSpout extends BaseRichSpout {

	/*
	 * API key:Gxo0x888U5shPrbQGdRqaJpQ7 API secret
	 * key:U073eJ75Touh9q4h4fX1CgfnVjlMTEhiHDXZyY6xv8dmRQADqI
	 */

	private static String consumerapikey;
	private static String consumerapisecretkey;
	private static String accesstoken;
	private static String accesstokensecret;
	private static String query;

	private TwitterStream tweetstream;
	private FilterQuery tweetfilterquery;
	private SpoutOutputCollector myCollector;
	private LinkedBlockingQueue<Status> queue;

	/*
	 * Constructor to set API token/key AND a filterquery
	 */

	public TweetSpout(FilterQuery filterQuery) {
		consumerapikey = "Gxo0x888U5shPrbQGdRqaJpQ7";
		consumerapisecretkey = "U073eJ75Touh9q4h4fX1CgfnVjlMTEhiHDXZyY6xv8dmRQADqI";
		accesstoken = "1232057111284051968-ydJc5G955U4csQP3BrTkveEOmuMG8B";
		accesstokensecret = "82SYQog09HgbzcM91aBb6nTd0bD4AChFjt7YisOp5tvML";
		tweetfilterquery = filterQuery;
	}

	/*
	 * declares that output will be of a custom-made type called Tweet.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("Tweet"));
	}

	@Override
	public void open(Map<String, Object> map, TopologyContext topologyContext,
			SpoutOutputCollector spoutOutputCollector) {
		this.queue = new LinkedBlockingQueue<Status>(1000);
		this.myCollector = spoutOutputCollector;

		StatusListener listener = new StatusListener() {

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			@Override
			public void onStatus(Status status) {
				if (status == null) {
					Utils.sleep(50);
				} else {
					queue.offer(status);
					System.out.println("@" + status.getUser().getScreenName() + " - " +
					status.getText());
				}
			}

			@Override
			public void onStallWarning(StallWarning warning) {
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			}
		};

		tweetstream = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		tweetstream.addListener(listener);
		tweetstream.setOAuthConsumer(consumerapikey, consumerapisecretkey);
		AccessToken token = new AccessToken(accesstoken, accesstokensecret);
		tweetstream.setOAuthAccessToken(token);

		// System.out.println("Access Token Object:" + token);

		if (tweetfilterquery == null) {
			tweetstream.sample();
		} else {
			// System.out.println("Ya boi is gonna be filtered");
			tweetstream.filter(tweetfilterquery);
		}

	}

	/**
	 * When requested for next tuple, reads message from queue and emits the
	 * message.
	 */
	/*
	 * // @Override // public void nextTuple() { // // emit tweets // Status ret =
	 * queue.poll(); // if (ret == null) { // Utils.sleep(1000); // } else { //
	 * System.out.println("Queue at "+ DateTime.now().toString() + ret); //
	 * this.myCollector.emit(new Values(ret)); // // } // }
	 */

	@Override
	public void nextTuple() {
		try {
			Status message = queue.poll();
			if (message == null) {
				// didn't get a message, sleep for a little bit
				Utils.sleep(50);
			} else {
				System.out.println("Queue at "+ DateTime.now().toString() + message.getText());
				this.myCollector.emit(new Values(message));
				//System.out.println("Emett emitted the stuff");
			}
		} catch (Exception e) {
			myCollector.reportError(e);
		}
	}

	@Override
	public void close() {
		tweetstream.shutdown();
		super.close();
	}

}
