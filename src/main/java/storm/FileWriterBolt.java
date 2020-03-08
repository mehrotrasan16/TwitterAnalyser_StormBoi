package main.java.storm;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Calendar;
import java.util.Date;
import java.util.Formatter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;


public class FileWriterBolt extends BaseRichBolt {
	
	/*
	 * Log file writers
	 */
	PrintWriter writer;
	FileWriter appender;
	
	/*
	 * Date annd timestamp related data members
	 */
	final String DATE_FORMAT = "dd_MM_yyyy_hh_mm_ss";
	SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
	Calendar currentTime = Calendar.getInstance();

	int count = 0;
	private OutputCollector myCollector;
	private String filename;
	

	public FileWriterBolt(String filename) {
		String timeStr = formatter.format(currentTime.getTime());
		this.filename = filename + timeStr + ".txt" ;
	}
	
	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
		myCollector = outputCollector;
		
		System.out.println("Ya boi is inside fileWriterBolt.prepare()");
		
		try {
			appender = new FileWriter(filename, true);
			writer = new PrintWriter(appender);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

	@Override
	public void execute(Tuple tuple) {
		System.out.println("FileWriterBolt executed");
		System.out.println(count);
		System.out.println(tuple);
		System.out.println(writer);
		if( tuple != null) {
		writer.println((count++) + ":" + tuple.getStringByField("hashtag"));
		writer.flush();
		myCollector.ack(tuple);
		}
	}
	
	@Override
	public void cleanup() {
		writer.close();
		super.cleanup();

	}

}
