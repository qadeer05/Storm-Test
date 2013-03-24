package spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RandomString extends BaseRichSpout {

	private SpoutOutputCollector collector;
	
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}
	
	public void close() {}
	
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}

	/**
	 * The only thing that the methods will do It is emit each 
	 * file line
	 */
	public void nextTuple() {
		/**
		 * The nextuple it is called forever, so if we have been readed the file
		 * we will wait and then return
		 */
			try {
				//Thread.sleep(1000);
				String returnString = "";
				List<String> lettersList = new ArrayList<String>();
				lettersList.add("G");
				lettersList.add("O");
				lettersList.add("T");
				Random randomNum = new Random();
				for (int i=0; i <3; i++){
					Random rnd = new Random();
					returnString += lettersList.get(rnd.nextInt(3));
				}
				this.collector.emit(new Values(returnString),returnString);
				//Thread.sleep(1000);
			} catch (Exception e) {
				//Do nothing
			}
			return;
	}

	/**
	 * We will create the file and get the collector object
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * Declare the output field "word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}

