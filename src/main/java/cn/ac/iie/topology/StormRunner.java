package cn.ac.iie.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public final class StormRunner{
	private static final int MILLS_IN_SEC = 1000;
	
	private StormRunner() {}
	
	public static void runTopologyLocally(TopologyBuilder builder, String topologyName, Config conf, int runtimeInSeconds) throws InterruptedException {
		
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology(topologyName, conf, builder.createTopology());
		Thread.sleep((long) runtimeInSeconds * MILLS_IN_SEC); 
		localCluster.shutdown();
		
	}
	
	public static void runTopologyRemotely(TopologyBuilder builder, String topologyName, Config conf ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
		StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
	}
	

}
