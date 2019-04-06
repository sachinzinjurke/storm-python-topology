package com.bny.ppe.spouts;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bny.common.storm.components.spouts.GenericSpout;

public class RandomSentenceSpout extends GenericSpout implements IRichSpout {

	private static final Logger logger = LoggerFactory.getLogger(RandomSentenceSpout.class.getName());

	SpoutOutputCollector _collector;
	Random _rand;

	public RandomSentenceSpout() {

	}

	public RandomSentenceSpout(String componentId) {
		super(componentId);
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		_rand = new Random();
	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public void activate() {
		// TODO Auto-generated method stub

	}

	public void deactivate() {
		// TODO Auto-generated method stub

	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(100);
		String[] sentences = new String[] { sentence("the cow jumped over the moon"),
				sentence("an apple a day keeps the doctor away"), sentence("four score and seven years ago"),
				sentence("snow white and the seven dwarfs"), sentence("i am at two with nature") };
		final String sentence = sentences[_rand.nextInt(sentences.length)];

		logger.info("Emitting tuple: {}", sentence);

		_collector.emit(new Values(sentence));
	}

	protected String sentence(String input) {
		return input;
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void addToTopology(TopologyBuilder builder) {
		// TODO Auto-generated method stub

	}

	// Add unique identifier to each tuple, which is helpful for debugging
	public static class TimeStamped extends RandomSentenceSpout {
		private final String prefix;

		public TimeStamped() {
			this("");
		}

		public TimeStamped(String prefix) {
			this.prefix = prefix;
		}

		protected String sentence(String input) {
			return prefix + currentDate() + " " + input;
		}

		private String currentDate() {
			return new SimpleDateFormat("yyyy.MM.dd_HH:mm:ss.SSSSSSSSS").format(new Date());
		}
	}
}
