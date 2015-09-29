package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import structures.CustomMatch;
import structures.CustomMsg;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by georskou on 28/09/15.
 */
public class ResultReducer implements IRichBolt {

    private OutputCollector collector;
    private HashMap<Integer, CustomMatch> badMessages;
    private int inst = 0;

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.badMessages = new HashMap<Integer, CustomMatch>();
    }


    public void execute(Tuple input) {
        Integer id = input.getInteger(0);
        Integer matchid = input.getInteger(1);
        Integer lineId = input.getInteger(2);
        Integer charId = input.getInteger(3);
        Integer matchId = input.getInteger(4);
        String word = input.getString(5);
        Integer newId = (String.valueOf(lineId) + "-" + String.valueOf(matchId)).hashCode();
        inst++;
        if (!badMessages.containsKey(newId)) {
            CustomMsg customMsg = new CustomMsg(id, lineId, charId, word);
            CustomMatch customMatch = new CustomMatch(matchid, customMsg, matchId);
            badMessages.put(newId, customMatch);
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
        for(Map.Entry<Integer, CustomMatch> entry : badMessages.entrySet()){
            System.out.println("## BAD ## " + entry.getKey() + ":" + entry.getValue());
        }
        System.out.println();
        System.out.println("#####################");
        System.out.println("(Size " + badMessages.size() +") out of " + inst);
        System.out.println("#####################");
        System.out.println();
    }
}
