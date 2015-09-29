package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import structures.CustomMatch;
import structures.CustomMsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class StreamMatcher implements IRichBolt {

    private OutputCollector collector;
    private HashMap<Integer, CustomMsg> messages;
    private HashMap<Integer, CustomMatch> badMessages;

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.messages = new HashMap<Integer, CustomMsg>();
        this.badMessages = new HashMap<Integer, CustomMatch>();
    }


    public void execute(Tuple input) {
        Integer id = input.getInteger(0);
        Integer lineId = input.getInteger(1);
        Integer charId = input.getInteger(2);
        String msg = input.getString(3);
        CustomMsg customMsg = new CustomMsg(id, lineId, charId, msg);
        messages.put(id, customMsg);

        ArrayList<Integer> foundIndeces = new ArrayList<Integer>();
        int index = msg.indexOf("and");
        while (index >= 0) {
            foundIndeces.add(index);
            index = msg.indexOf("and", index + 1);
        }
        for (int foundInd : foundIndeces) {
            int matchID = new String(customMsg.lineId + "-" + customMsg.charId + foundInd).hashCode();
            CustomMatch customMatch = new CustomMatch(matchID, customMsg, customMsg.charId  + foundInd);
            badMessages.put(matchID, customMatch);
            collector.emit(new Values(customMsg.id, customMatch.id, customMsg.lineId, customMsg.charId, customMatch.matchId, customMsg.msg));
        }
        collector.ack(input);
    }


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "matchid", "line", "char", "match", "word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void cleanup() {
        /*System.out.println();
        System.out.println("#####################");
        System.out.println("Stream Matcher Report");
        System.out.println("#####################");
        System.out.println();
        System.out.println("-- Messages -- (Size " + messages.size() +")");
        for(Map.Entry<String, String> entry : messages.entrySet()){
            System.out.println("## MSG ## " + entry.getKey() + ":" + entry.getValue());
        }
        System.out.println();
        System.out.println("#####################");
        System.out.println("(Size " + messages.size() +")");
        System.out.println("#####################");
        System.out.println();*/
    }

}
