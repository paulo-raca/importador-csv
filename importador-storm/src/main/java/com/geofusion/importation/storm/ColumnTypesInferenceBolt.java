package com.geofusion.importation.storm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

import com.geofusion.importation.ColumnType;
import com.geofusion.importation.ColumnTypeInference;
import com.geofusion.importation.RecordTypeInference;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;

import backtype.storm.task.TopologyContext;
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
public class ColumnTypesInferenceBolt extends BaseBasicBolt {
	public static final MongoClient MONGO = new MongoClient();
	Map<MongoId, RecordTypeInference> aggregations;
	
	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		aggregations = new HashMap<>();
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) throws FailedException {
		if ("records".equals(input.getSourceStreamId())) {
			MongoId mongoId = (MongoId)input.getValueByField("mongoId");
			CSVRecord record = (CSVRecord)input.getValueByField("record");
			
			if (!aggregations.containsKey(mongoId)) {
				aggregations.put(mongoId, new RecordTypeInference());
			}
			aggregations.get(mongoId).digest(record);
			
			
		} else if ("flush".equals(input.getSourceStreamId())) {
			for (Map.Entry<MongoId, RecordTypeInference> entry : aggregations.entrySet()) {
				MongoId mongoId = entry.getKey();
				List<ColumnTypeInference> columns = entry.getValue().getColumns();
				
				BasicDBObject $inc = new BasicDBObject();
				for (int i=0; i<columns.size(); i++) {
					for (ColumnType type : ColumnType.values()) {
						$inc.append("columns." + i + ".typeMatches." + type, columns.get(i).getMatches().get(type));
					}
				}
				MONGO
					.getDatabase(mongoId.getDatabase())
					.getCollection(mongoId.getCollection())
					.updateOne(
							new BasicDBObject("_id", mongoId.get_id()),
							new BasicDBObject().append("$inc", $inc));
			}
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
