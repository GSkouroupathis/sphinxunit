package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class StreamMatcher implements IRichBolt {

    private OutputCollector collector;
    private HashMap<Integer, String> messages;
    private HashMap<Integer, String> badMessages;

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.messages = new HashMap<Integer, String>();
        this.badMessages = new HashMap<Integer, String>();
    }


    public void execute(Tuple input) {
        Integer id = input.getInteger(0);
        String msg = input.getString(1);
        messages.put(id, msg);

        if (msg.contains("and")) {
            badMessages.put(id, msg);
            collector.emit(new Values(id, msg));
        }
        collector.ack(input);
    }


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void cleanup() {
        System.out.println();
        System.out.println("#####################");
        System.out.println("Stream Matcher Report");
        System.out.println("#####################");
        System.out.println();
        System.out.println("-- Messages -- (Size " + messages.size() +")");
        for(Map.Entry<Integer, String> entry : messages.entrySet()){
            System.out.println("## MSG ## " + entry.getKey() + ":" + entry.getValue());
        }
        System.out.println();
        System.out.println("#####################");
        System.out.println("(Size " + messages.size() +")");
        System.out.println("#####################");
        System.out.println();
    }

}
