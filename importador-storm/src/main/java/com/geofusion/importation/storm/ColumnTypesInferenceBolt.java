package com.geofusion.importation.storm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

import com.geofusion.importation.RecordTypeInference;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Receve montes de CSVRecords e verifica quais tipos de dados são compatíveis com cada coluna.
 * O resultado agregado é armazenado internamente.
 * Quando é recebida uma mensagem "flush", a agregação interna é gravada no Mongo 
 * (de forma incremental, para permitindo concorrencia)
 */
@SuppressWarnings("serial")
public class ColumnTypesInferenceBolt extends BaseBasicBolt {
	Map<Object, Aggregation> aggregations;

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		aggregations = new HashMap<>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) throws FailedException {
		if ("records".equals(input.getSourceStreamId())) {
			Object id = input.getValueByField("id");
			CSVRecord record = (CSVRecord)input.getValueByField("record");

			if (!aggregations.containsKey(id)) {
				aggregations.put(id, new Aggregation());
			}
			aggregations.get(id).recordType.digest(record);
			aggregations.get(id).tuples.add(input);


		} else if ("recordType".equals(input.getSourceStreamId())) {
			Object id = input.getValueByField("id");
			RecordTypeInference recordType = (RecordTypeInference)input.getValueByField("recordType");

			if (!aggregations.containsKey(id)) {
				aggregations.put(id, new Aggregation());
			}
			aggregations.get(id).recordType.digest(recordType);
			aggregations.get(id).tuples.add(input);	

		} else if (input.getSourceStreamId().startsWith("flush")) {
			if (input.getFields().contains("id")) {
				Object id = input.getValueByField("id");
				Aggregation aggregation = aggregations.remove(id);
				if (aggregation != null) {
					collector.emit("recordType", Arrays.<Object>asList(id, aggregation.recordType));	
				}
			} else {		
				for (Map.Entry<Object, Aggregation> entry : aggregations.entrySet()) {
					collector.emit("recordType", Arrays.<Object>asList(entry.getKey(), entry.getValue().recordType));
				}
				aggregations.clear();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("recordType", new Fields("id", "recordType"));
	}

	class Aggregation {
		RecordTypeInference recordType = new RecordTypeInference();
		List<Tuple> tuples = new LinkedList<>();
	}
}
