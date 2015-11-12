package com.geofusion.importation.storm;

import java.util.Arrays;
import java.util.Map;

import org.bson.Document;

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
public class FileSpout extends BaseRichSpout {
	public static final MongoClient MONGO = new MongoClient();
	String mongoDatabase, mongoCollection;
	SpoutOutputCollector collector;
	
	public FileSpout(String mongoDatabase, String mongoCollection) {
		this.mongoDatabase = mongoDatabase;
		this.mongoCollection = mongoCollection;
	}
	
    @SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }
    
	@Override
	public void nextTuple() {
		Document next = MONGO
			.getDatabase(mongoDatabase)
			.getCollection(mongoCollection)
			.findOneAndUpdate(
					new BasicDBObject("state", ImportationState.CREATED.toString()),
					new BasicDBObject("$set", new BasicDBObject("state", ImportationState.PARSING.toString())));
		if (next != null) {
			MongoId id = new MongoId(mongoDatabase, mongoCollection, next.get("_id"));
			collector.emit("sources", Arrays.<Object>asList(id, next.getString("source"), null), new Stage(id, 1));
		}
	}
	
	@Override
	public void ack(Object msgId) {
		Stage stage = (Stage) msgId;
		if (stage.stage == 1) {
			MONGO
					.getDatabase(mongoDatabase)
					.getCollection(mongoCollection)
					.updateOne(
							new BasicDBObject("_id", stage.mongoId),
							new BasicDBObject("$set", new BasicDBObject("state", ImportationState.PARSED.toString())));
			collector.emit("flush", Arrays.<Object>asList(stage.mongoId), new Stage(stage.mongoId, 2));
			System.err.println("Finished Phase 1 - Flushing");
			
		} else if (stage.stage == 2) {
			MONGO
					.getDatabase(mongoDatabase)
					.getCollection(mongoCollection)
					.updateOne(
							new BasicDBObject("_id", stage.mongoId),
							new BasicDBObject("$set", new BasicDBObject("state", ImportationState.FINISHED.toString())));
			System.err.println("Finished Phase 2 - Done");
		}
	}
	
	@Override
	public void fail(Object msgId) {
		Stage stage = (Stage) msgId;
		MONGO
				.getDatabase(stage.mongoId.getDatabase())
				.getCollection(stage.mongoId.getCollection())
				.updateOne(
						new BasicDBObject("_id", stage.mongoId.get_id()),
						new BasicDBObject("$set", new BasicDBObject("state", ImportationState.FAILED.toString())));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("sources", new Fields("mongoId", "source", "maxRecords"));
		declarer.declareStream("flush", new Fields("mongoId"));
	}
	
	private class Stage {
		public MongoId mongoId;
		public int stage;
		
		public Stage(MongoId mongoId, int stage) {
			this.mongoId = mongoId;
			this.stage = stage;
		}
	}
}
