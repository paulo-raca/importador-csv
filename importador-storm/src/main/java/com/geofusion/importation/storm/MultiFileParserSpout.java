package com.geofusion.importation.storm;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

import com.google.common.io.CountingInputStream;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * Espera até que novas importações sejam inseridas no mongo.
 * Quando isso ocorre, é realizado um processamento em várias fases:
 * - O estado da importação é alterado para "PARSING"
 * - É emitido o URL do arquivo para ser parseado, fazendo com que os bolts processem todo o arquivo
 * - Ao completar o processamento, o estado da importação é alterado para "PARSED"
 * - É emitida uma mensagem "flush", fazendo com que os bolts salvem o resultado da agregação no mongo
 * - Ao completar a agregação, o estado da importação é alterado para "FINISHED"
 */
@SuppressWarnings("serial")
public class MultiFileParserSpout extends BaseRichSpout {
	public static final MongoClient MONGO = new MongoClient();
	String mongoDatabase, mongoCollection;
	SpoutOutputCollector _collector;
	Map<MongoId, OpenFile> openFiles = new HashMap<>(); 
	BlockingQueue<OpenFile> roundRobin = new LinkedBlockingQueue<>();
	long nextRefresh = System.currentTimeMillis();
	
	public MultiFileParserSpout(String mongoDatabase, String mongoCollection) {
		this.mongoDatabase = mongoDatabase;
		this.mongoCollection = mongoCollection;
	}
	
    @SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
    }
    
    private void emit(OpenFile file, String stream, Object... data) {
    	List<Object> list = new ArrayList<>();
    	list.add(file.mongoId);
    	for (Object o : data) {
    		list.add(o);
    	}
		file.pending++;
		_collector.emit(stream, list, file.mongoId);
    }
    
    @Override
    public synchronized void nextTuple() {
    	//Detect and open new files
    	if (openFiles.isEmpty() || (System.currentTimeMillis() >= nextRefresh)) {
    		Document next = MONGO
    				.getDatabase(mongoDatabase)
    				.getCollection(mongoCollection)
    				.findOneAndUpdate(
    						new BasicDBObject("state", ImportationState.CREATED.toString()),
    						new BasicDBObject("$set", new BasicDBObject("state", ImportationState.PARSING.toString())));

    		nextRefresh = System.currentTimeMillis() + (next == null ? 5000 : 0);
    		if (next != null) {
    			MongoId mongoId = new MongoId(mongoDatabase, mongoCollection, next.get("_id"));
    			OpenFile file = new OpenFile();
    			file.mongoId = mongoId;
    			try {
	    			URL source =  new URL(next.getString("source"));
	    			URLConnection conn = source.openConnection();	
	    			file.fileSize = conn.getContentLengthLong();
	    			file.countingInput = new CountingInputStream(conn.getInputStream());
	    			file.parser = CSVFormat.DEFAULT.withHeader().parse(new InputStreamReader(file.countingInput, "UTF-8"));
	    			file.iterator = file.parser.iterator();
	    			file.state = OpenFileState.PARSING;
	    			
	    			roundRobin.add(file);
	    			openFiles.put(mongoId, file);
	    			
    			} catch (IOException e) {
    				file.state = OpenFileState.FAILED;
    			}
    			file.syncMongo();
    		}
    	}
    	
    	OpenFile file = roundRobin.poll();
    	if (file != null && file.state == OpenFileState.PARSING) {
    		if (file.iterator.hasNext()) {
    			long pos = file.countingInput.getCount();
    			CSVRecord record = file.iterator.next();
    			emit(file, "records", record, file.countingInput.getCount() - pos);
    		}
    		
    		if (!file.iterator.hasNext()) {
    			file.state = OpenFileState.PARSED;
    			file.syncMongo();
    			System.err.println("Finished Parsing");
    			checkParsed(file);
    		} else {
    			roundRobin.add(file);
    		}
    	}
    }
    
    public void checkParsed(OpenFile file) {
		if (file.state == OpenFileState.PARSED && file.pending == 0) {
			file.state = OpenFileState.AGGREGATING;
			System.err.println("Aggregating...");
			emit(file, "flush");
		}
    }
    
    public void checkFinished(OpenFile file) {
		if (file.state == OpenFileState.AGGREGATING && file.pending == 0) {
			file.state = OpenFileState.FINISHED;
			System.err.println("Finished!");
			file.syncMongo();
			openFiles.remove(file.mongoId);
		}
    }
	
	@Override
	public synchronized void ack(Object msgId) {
		MongoId mongoId = (MongoId) msgId;
		OpenFile file = openFiles.get(mongoId);
		if (file == null) {
			return;
		}
		file.pending--;
		if (file.state == OpenFileState.PARSING) {
			file.processed++;
			if (file.processed % 100 == 0) {
				System.out.print(".");
				if (file.processed % 5000 == 0) {
					System.out.println();
	    			file.syncMongo();
				}
			}
		}
		checkParsed(file);
		checkFinished(file);
	}
	
	@Override
	public synchronized void fail(Object msgId) {
		MongoId mongoId = (MongoId) msgId;
		OpenFile file = openFiles.get(mongoId);
		if (file == null) {
			return;
		}
		file.pending--;
		file.state = OpenFileState.FAILED;
		file.syncMongo();
		openFiles.remove(file.mongoId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("records", new Fields("id", "record", "bytes"));
		declarer.declareStream("flush", new Fields("id"));
	}
	
	enum OpenFileState {
		PARSING, PARSED, AGGREGATING, FINISHED, FAILED;
	}
	class OpenFile {
		MongoId mongoId;
		OpenFileState state = OpenFileState.PARSING;
		CSVParser parser;
		Iterator<CSVRecord> iterator;
		long fileSize;
		CountingInputStream countingInput;
		
		int processed=0, pending=0;
		
		private void syncMongo() {
			BasicDBObject $set = new BasicDBObject();
			$set.append("state", state.toString());
			$set.append("parsePercent", processed==0 ? 0 : 100.0 * (1.0*processed/(parser.getRecordNumber()-1.0))*(1.0*countingInput.getCount()/fileSize));
			/*$set.append("processedBytes", countingInput.getCount());
			$set.append("state", state.toString());*/
			MONGO
				.getDatabase(mongoId.getDatabase())
				.getCollection(mongoId.getCollection())
				.updateOne(
						new BasicDBObject("_id", mongoId.get_id()),
						new BasicDBObject("$set", $set));
		}
	}
}
