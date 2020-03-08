package main.java.storm;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.FilterQuery;

/*//import java.util.logging.Level;
//import java.util.logging.LogManager;
//import java.util.logging.Logger;
*/

public class DriverTopology {
	// this class will handle the output of the bolts and the spouts

	public static void main(String[] args) throws Exception {

		/*
		 * Accept Arguments
		 */
		String remoteClusterTopologyName = null;
		if (args != null) {
			if (args.length == 1) {
				remoteClusterTopologyName = args[0];
			}
		}

		/*?
		 * Initalize Topology builder and other required variables
		 * Add a location and language to the tweetquery to filter tweets from specified location and in that language.
		 * Adding track searches for hashtag filters by hasgtag
		 */
		TopologyBuilder builderBoi = new TopologyBuilder();
		
		FilterQuery tweetQuery = new FilterQuery();		
		tweetQuery.locations(new double[][]{new double[]{3.339844, 53.644638},
            new double[]{18.984375,72.395706
            }});
		tweetQuery.language("en");		
		//Filter on hashtags
		tweetQuery.track(new String[]{"#engineering", "#bigdata", "#rockon","#love","#coffee", "#music"});
		// Logger logboi = LoggerFactory.getLogger(DriverTopology.class);

		Config conf = new Config();
		conf.setDebug(false);
		
		
		builderBoi.setSpout("spout", new TweetSpout(tweetQuery));		
		builderBoi.setBolt("hashtag-extractor", new TweetExtractionBolt(), 4).shuffleGrouping("spout");
		builderBoi.setBolt("file-writer", new FileWriterBolt("tweetslogfile"), 1).shuffleGrouping("hashtag-extractor");
		builderBoi.setBolt("lossy-counter", new LossyCountBolt()).fieldsGrouping("hashtag-extractor", new Fields("hashtag"));

		if (remoteClusterTopologyName != null) {
			conf.setNumWorkers(3);
			try {
				StormSubmitter.submitTopology("Cluster-twitter-func", conf, builderBoi.createTopology());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			try {
				cluster.submitTopology("twitter-func", conf, builderBoi.createTopology());
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			Thread.sleep(60000);

			//cluster.shutdown();
		}

	}

}
