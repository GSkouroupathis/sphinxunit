package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class StreamMatcher implements IRichBolt {

    private OutputCollector collector;
    private ArrayList<String> messages;
    private ArrayList<String> badMessages;

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.messages = new ArrayList<String>();
        this.badMessages = new ArrayList<String>();
    }


    public void execute(Tuple input) {
        String str = input.getString(0);
        messages.add(str);
        collector.ack(input);

        if (str.contains("aaa") || str.contains("tbi")) {
            badMessages.add(str);
        }
    }


	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void cleanup() {
        System.out.println("-- Stream Matcher --");
        for(String entry : messages){
            System.out.println("#####-->>" + entry);
        }
        System.out.println("####<<<<" + messages.size());
        System.out.println("-- Bad Messages --");
        for(String entry : badMessages){
            System.out.println("XXXXXX-->>" + entry);
        }

    }

}
