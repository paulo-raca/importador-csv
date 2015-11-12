package com.geofusion.importation.storm;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.geofusion.importation.ColumnType;
import com.google.common.io.CountingInputStream;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class ParseFileBolt extends BaseBasicBolt {
	public static final MongoClient MONGO = new MongoClient();
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) throws FailedException {		
		try {
			MongoId mongoId = (MongoId)input.getValueByField("mongoId");
			URL source =  new URL(input.getStringByField("source"));
			Long maxRecords = input.getLongByField("maxRecords");
			
			URLConnection conn = source.openConnection();
			long totalBytes = conn.getContentLengthLong();		
			CountingInputStream countingInput = new CountingInputStream(conn.getInputStream());
			
			try (CSVParser parser = CSVFormat.DEFAULT.withHeader().parse(new InputStreamReader(countingInput, "UTF-8"))) {
				long currentPosition = countingInput.getCount();
				long totalRecords = 0;
				syncMongo(mongoId, totalBytes, currentPosition, -1L, 0L, new ArrayList<String>(parser.getHeaderMap().keySet()));
				
				for (CSVRecord record : parser) {
					if (maxRecords != null && parser.getRecordNumber() > maxRecords) {
						break;
					}
					
					collector.emit("rows", Arrays.<Object>asList(mongoId, record, countingInput.getCount() - currentPosition));
					totalRecords++;
					currentPosition = countingInput.getCount();
					if ((record.getRecordNumber() % 50000) == 0) {
						System.err.println(". " + (1000*currentPosition/totalBytes)/10.0 + "%" );
						collector.emit("flush", Arrays.<Object>asList(mongoId));
					}
				}
				syncMongo(mongoId, countingInput.getCount(), null, totalRecords, null, null);
			}
		} catch (IOException e) {
			throw new FailedException(e);
		}
	}
	
	private void syncMongo(MongoId mongoId, Long totalBytes, Long processedBytes, Long totalRecords, Long processedRecords, List<String> columns) {
		BasicDBObject $set = new BasicDBObject();
		if (totalBytes != null) {
			$set.append("totalBytes", totalBytes < 0 ? null : totalBytes);
		}
		if (processedBytes != null) {
			$set.append("processedBytes", processedBytes < 0 ? null : processedBytes);
		}
		if (totalRecords != null) {
			$set.append("totalRecords", totalRecords < 0 ? null : totalRecords);
		}
		if (processedRecords != null) {
			$set.append("processedRecords", processedRecords < 0 ? null : processedRecords);
		}
		if (columns != null) {
			BasicDBList dbColumns = new BasicDBList();
			$set.append("columns", dbColumns);
			for (String columnName : columns) {
				BasicDBObject dbColumn = new BasicDBObject();
				BasicDBObject dbTypeMatches = new BasicDBObject();
				dbColumns.add(dbColumn);
				dbColumn.put("name", columnName);
				dbColumn.put("type", ColumnType.STRING.toString());
				dbColumn.put("typeMatches", dbTypeMatches);
				for (ColumnType type : ColumnType.values()) {
					dbTypeMatches.put(type.toString(), 0L);
				}
			}
		}
		MONGO
			.getDatabase(mongoId.getDatabase())
			.getCollection(mongoId.getCollection())
			.updateOne(
					new BasicDBObject("_id", mongoId.get_id()),
					new BasicDBObject("$set", $set));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("rows", new Fields("mongoId", "record", "bytes"));
		declarer.declareStream("flush", new Fields("mongoId"));
	}
}
