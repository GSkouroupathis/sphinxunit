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

public class StreamReader extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private HashMap<String, String> toSend = new HashMap<String, String>();
	private HashMap<String, String> messages = new HashMap<String, String>();
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
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			//return;
		}
		if(!toSend.isEmpty()){
			for(Map.Entry<String, String> transactionEntry : toSend.entrySet()){
				String trId = transactionEntry.getKey();
				String trMsg = transactionEntry.getValue();
				collector.emit(new Values(trId, trMsg), trId);//,trId
			}
			/*
			 * The nextTuple, ack and fail methods run in the same loop, so
			 * we can considerate the clear method atomic
			 */
			toSend.clear();
		}
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			// nada
		} finally {
			completed = true;
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
				lineInd = 0;
				msgInd = 0;
				count = 0;
				chrInd = 0;
				while((line = reader.readLine()) != null) {

					while (chrInd < line.length() && count < messageSize) {
						chunkBuilder.append(line.charAt(chrInd));
						count++;
						chrInd++;
						if (count == messageSize) {
							messages.put(  Integer.toString(lineInd) + "-" + Integer.toString(msgInd), chunkBuilder.toString() );
							chunkBuilder.setLength(0);
							count = 0;
							msgInd++;
							chrInd -= overlapSize;
						}
					}
					// Finally send remaining bits
					if (chunkBuilder.length() != 0) {
						messages.put(  Integer.toString(lineInd) + "-" + Integer.toString(msgInd), chunkBuilder.toString() );
						chunkBuilder.setLength(0);
						count = 0;
					}

					toSend.putAll(messages);
					lineInd++;
					msgInd = 0;
					chrInd = 0;
				}
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
		declarer.declare(new Fields("id", "word"));
	}

	public void ack(Object msgId) {
		toSend.remove(msgId);
		System.out.println("## Successfully processed " + msgId);
	}

	public void fail(Object msgId) {
		System.out.println("## Failed to process " + msgId + ", retrying");
		toSend.put((String)msgId, messages.get(msgId));
	}

	public void close() {}
}
