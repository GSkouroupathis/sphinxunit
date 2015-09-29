package spouts;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Values;
import structures.CustomMsg;

public class StreamReader extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private HashMap<Integer, CustomMsg> toSend = new HashMap<Integer, CustomMsg>();
	private HashMap<Integer, CustomMsg> messages = new HashMap<Integer, CustomMsg>();
	private int chunkSize, overlapSize, messageSize;

	/*
	Constructor
	*/
	public StreamReader (int chunkSize, int overlapSize) {
		this.chunkSize = chunkSize;
		this.overlapSize = overlapSize;
		this.messageSize = chunkSize + overlapSize;
	}

	/**
	 * The only thing that the methods will do It is emit each 
	 * file line
	 */
	public void nextTuple() {
		/**
		 * The nextuple it is called forever, so if we have been readed the file
		 * we will wait and then return
		 */
		if (!toSend.isEmpty()){
			for (Map.Entry<Integer, CustomMsg> transEntry : toSend.entrySet()){
				Integer transId = transEntry.getKey();
				CustomMsg customMsg = transEntry.getValue();
				Integer lineId = customMsg.lineId;
				Integer charId = customMsg.charId;
				String msg = customMsg.msg;
				collector.emit(new Values(transId, lineId, charId, msg));
			}
			/*
			 * The nextTuple, ack and fail methods run in the same loop, so
			 * we can considerate the clear method atomic
			 */
			toSend.clear();
		}
		try {
			Thread.sleep(2);
		} catch (InterruptedException e) {
			// nada
		}
	}

	/**
	 * We will create the file and get the collector object
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("streamFile").toString());
			BufferedReader reader = new BufferedReader(fileReader);
			StringBuilder chunkBuilder = new StringBuilder();
			String line;
			int count, lineInd, msgInd, chrInd;
			try {
				lineInd = 1;
				msgInd = 0;
				count = 0;
				chrInd = 1;
				while((line = reader.readLine()) != null) {
					if (lineInd == 106) {
						System.out.print("");
					}
					while (chrInd <= line.length() && count < messageSize) {
						chunkBuilder.append(line.charAt(chrInd-1));
						count++;
						chrInd++;
						if (count == messageSize) {
							int id = new String(lineInd + "-" + msgInd).hashCode();
							messages.put(id, new CustomMsg(id, lineInd, msgInd*chunkSize, chunkBuilder.toString()));
							chunkBuilder.setLength(0);
							count = 0;
							msgInd++;
							chrInd -= overlapSize;
						}
					}
					// Finally send remaining bits
					if (chunkBuilder.length() != 0) {
						int id = new String(lineInd + "-" + msgInd).hashCode();
						messages.put(id, new CustomMsg(id, lineInd, msgInd*chunkSize, chunkBuilder.toString()));
						chunkBuilder.setLength(0);
						count = 0;
					}
					lineInd++;
					msgInd = 0;
					chrInd = 1;
				}
				toSend.putAll(messages);
			} catch (IOException e) {
				throw new RuntimeException("Error reading line: " + e.getMessage());
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+conf.get("streamFile")+"]");
		}
		this.collector = collector;
	}

	/**
	 * Declare the output field "word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "line", "char", "word"));
	}

	public void ack(Object msgId) {
		toSend.remove(msgId);
		System.out.println("## Successfully processed " + msgId);
	}

	public void fail(Object msgId) {
		System.out.println("## Failed to process " + msgId + ", retrying");
		toSend.put((Integer)msgId, messages.get(msgId));
	}

	public void close() {}
}
