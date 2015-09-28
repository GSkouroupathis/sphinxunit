import bolts.ResultReducer;
import spouts.StreamReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import bolts.StreamMatcher;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("stream-reader", new StreamReader(1,2));
		builder.setBolt("stream-matcher", new StreamMatcher(), 1)
			.shuffleGrouping("stream-reader");
		builder.setBolt("result-reducer", new ResultReducer(), 1)
			.shuffleGrouping("stream-matcher");

        //Configuration
		Config conf = new Config();
		conf.put("streamFile", args[0]);
		conf.setDebug(false);

        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("SphinxUnit topology", conf, builder.createTopology());
		Thread.sleep(2000);
		cluster.shutdown();
	}
}
