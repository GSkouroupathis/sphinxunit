package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by georskou on 28/09/15.
 */
public class ResultReducer implements IRichBolt {

    private OutputCollector collector;
    private HashMap<String, String> badMessages;
    private int inst = 0;

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.badMessages = new HashMap<String, String>();
    }


    public void execute(Tuple input) {
        String id = input.getString(0);
        String msg = input.getString(1);
        inst++;
        if (!badMessages.containsKey(id)) {
            badMessages.put(id, msg);
        }
        collector.ack(input);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void cleanup() {
        System.out.println();
        System.out.println("#####################");
        System.out.println("Result Reducer Report");
        System.out.println("#####################");
        System.out.println();
        System.out.println("-- Bad Messages -- (Size " + badMessages.size() +")");
        for(Map.Entry<String, String> entry : badMessages.entrySet()){
            System.out.println("## BAD ## " + entry.getKey() + ":" + entry.getValue());
        }
        System.out.println();
        System.out.println("#####################");
        System.out.println("(Size " + badMessages.size() +") out of " + inst);
        System.out.println("#####################");
        System.out.println();
    }
}
