package com.bny.ppe.bolts;

import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PythonBolt extends ShellBolt implements IRichBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(PythonBolt.class.getName());
	public PythonBolt() {
        super("python", "D:\\workspaces\\multi-lang-storm\\workspace\\storm-python-topology\\multilang\\resources\\splitsentence.py");
        logger.info("PYTHON BOLT INITIALIZED...******************");
    }
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ShellBolt setEnv(Map<String, String> env) {
		logger.info("calling env*********");
		return super.setEnv(env);
	}

}
