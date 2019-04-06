package com.bny.ppe.runner;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.bny.common.storm.topology.SingleTopologySubmission;
import com.bny.common.storm.topology.TopologySubmission;

public final class LocalTopologySubmitter {

	private static final Logger LOGGER = LoggerFactory.getLogger(LocalTopologySubmitter.class.getName());

	private LocalTopologySubmitter() {
	}

	private static void validateArgs(final String[] args) {
		if(args==null || args.length==0){
			throw new IllegalArgumentException(
					"Arguments missing: 1: Context XML was not defined, Argument 2: Topology name was not defined"); 
		}
		if(args[0] == null) {
			throw new IllegalArgumentException(
					"Argument 1: Context XML was not defined"); 
		};
		if(args[1] == null) {
			throw new IllegalArgumentException(
					"Argument 2: Topology name was not defined"); 
		};
	}

	private void submitTopologies(final LocalCluster cluster, final TopologySubmission topologySubmission, String topologyName) {
		LOGGER.info("MP-CONTENT-OFFER :: Submit MP-CONTENT-OFFER Topology");
		for (Map.Entry<String, StormTopology> entry : topologySubmission.getStormTopologies().entrySet()) {
			if(StringUtils.equalsIgnoreCase(topologyName, entry.getKey())){
				StormTopology stormTopology = entry.getValue();
				cluster.submitTopology(entry.getKey(), topologySubmission.getConfig(), stormTopology);
				LOGGER.info("Successfully submitted topology :: "+entry.getKey());
			}		
		}
	}
	
	private void killTopologies(final LocalCluster cluster,	final TopologySubmission topologySubmission) {
		for (String key : topologySubmission.getStormTopologies().keySet()) {
			cluster.killTopology(key);
		}
	}

	public static void main(final String[] args) {
		validateArgs(args);
		final String context_xml = args[0]; //rt-mp-content-offer-context.xml
		final String topologyName = args[1]; //mpContentOfferLoaderTopology
		
		final ConfigurableApplicationContext context = new ClassPathXmlApplicationContext(context_xml);

		final TopologySubmission topologySubmission = (TopologySubmission) context.getBean(SingleTopologySubmission.class);
		final Integer runtime = NumberUtils.toInt("10000000");
		LOGGER.info("STORM PYTHON TOPOLOGY :: Before cluster started");
		final LocalCluster cluster = new LocalCluster();		
		final LocalTopologySubmitter localTopologySubmitter = (LocalTopologySubmitter) context.getBean("localTopologySubmitter");
		localTopologySubmitter.submitTopologies(cluster, topologySubmission, topologyName);

		
		LOGGER.info("*************************Cluster Started*************************");
		Utils.sleep(40000); // Let run for a bit
		LOGGER.info("*************************Started Cluster Shutdown*************************");
		localTopologySubmitter.killTopologies(cluster, topologySubmission);
		cluster.shutdown();
		context.close();
		LOGGER.info("*************************Cluster Shutdown Completed*************************");
	}

}
