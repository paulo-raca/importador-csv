package com.geofusion.importation.storm;

import com.geofusion.importation.ColumnType;
import com.geofusion.importation.RecordTypeInference;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Receve montes de CSVRecords e verifica quais tipos de dados são compatíveis com cada coluna.
 * O resultado agregado é armazenado internamente.
 * Quando é recebida uma mensagem "flush", a agregação interna é gravada no Mongo 
 * (de forma incremental, para permitindo concorrencia)
 */
@SuppressWarnings("serial")
public class SaveColumnTypesToMongoBolt extends BaseBasicBolt {
	public static final MongoClient MONGO = new MongoClient();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) throws FailedException {
		MongoId mongoId = (MongoId)input.getValueByField("id");
		RecordTypeInference recordType = (RecordTypeInference)input.getValueByField("recordType");

		BasicDBObject $inc = new BasicDBObject();
		for (int i=0; i<recordType.getColumns().size(); i++) {
			for (ColumnType type : ColumnType.values()) {
				$inc.append("columns." + i + ".typeMatches." + type, recordType.getColumns().get(i).getMatches().get(type));
			}
		}
		MONGO
			.getDatabase(mongoId.getDatabase())
			.getCollection(mongoId.getCollection())
			.updateOne(
					new BasicDBObject("_id", mongoId.get_id()),
					new BasicDBObject().append("$inc", $inc));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
