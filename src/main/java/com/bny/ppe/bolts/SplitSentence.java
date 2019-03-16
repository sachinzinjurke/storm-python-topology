package com.bny.ppe.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bny.common.storm.components.bolt.GenericBolt;
import com.bny.ppe.runner.LocalTopologySubmitter;

public class SplitSentence extends GenericBolt implements IRichBolt{

	private static final Logger logger = LoggerFactory.getLogger(SplitSentence.class.getName());
	OutputCollector collector;
	
	public SplitSentence(){
	}
	
	public SplitSentence(String componentId) {
		super(componentId);
	}
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		
	}

	public void execute(Tuple input) {
		 String sentence = input.getStringByField("sentence");
		 logger.info("bolt got ::" + sentence);
	      String words[] = sentence.split(" ");
	      for (String w : words) {
	    	  collector.emit(new Values(w));
	      }
		
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public IComponent getStormBolt() {
		// TODO Auto-generated method stub
		return null;
	}

	public void addToTopology(TopologyBuilder builder) {
		// TODO Auto-generated method stub
		
	}

}
