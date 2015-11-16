package com.geofusion.importation.storm;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;
import java.util.UUID;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SubmitFile {
	private static String[] StringArrayType = {};
	public static void main(String[] args) throws IOException {
		ZkClient zkClient = new ZkClient("kafka:2181", 10000, 10000);
		Properties props = new Properties();
		props.put("metadata.broker.list", "kafka:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		Producer<String, String[]> producer = new Producer<String, String[]>(new ProducerConfig(props));

		for (String fileUrl : args) {
			String id = UUID.randomUUID().toString();
			String records_topic = "typeinference." + id + ".records";
			String types_topic = "typeinference." + id + ".types";
			
			AdminUtils.createTopic(zkClient, records_topic, 1, 1, new Properties());
			AdminUtils.createTopic(zkClient, types_topic, 1, 1, new Properties());
			
			CSVParser parser = CSVFormat.DEFAULT.withHeader().parse(new InputStreamReader(new URL(fileUrl).openStream(), "UTF-8"));
			for (CSVRecord record : parser) {
				producer.send(new KeyedMessage<String, String[]>(records_topic, record.toMap().values().toArray(StringArrayType)));
			}
			/*file.iterator = file.parser.iterator();
			file.state = OpenFileState.PARSING;*/
		}
	}
}
