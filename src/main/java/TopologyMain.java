import spouts.StreamReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import bolts.StreamMatcher;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("stream-reader", new StreamReader(4,3));
		builder.setBolt("stream-matcher", new StreamMatcher())
			.shuffleGrouping("stream-reader");
		
        //Configuration
		Config conf = new Config();
		conf.put("streamFile", args[0]);
		conf.setDebug(false);

        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("SphinxUnit topology", conf, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();
	}
}
