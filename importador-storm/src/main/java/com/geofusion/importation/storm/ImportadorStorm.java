package com.geofusion.importation.storm;

import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class ImportadorStorm {	
	public static void main(String[] args) throws IOException {
		TopologyBuilder metaTopologyBuilder = new TopologyBuilder();        

		metaTopologyBuilder.setSpout("source", new MultiFileParserSpout("storm", "import_meta"));
		metaTopologyBuilder.setSpout("flush", new TimedFlushSpout(2, 1000));
		metaTopologyBuilder.setBolt("inferTypes", new ColumnTypesInferenceBolt(), 4)
			.shuffleGrouping("source", "records")
			.allGrouping("flush", "flush-1");
		metaTopologyBuilder.setBolt("mergeTypes", new ColumnTypesInferenceBolt(), 3)
			.fieldsGrouping("inferTypes", "recordType", new Fields("id"))
			.allGrouping("flush", "flush-2");
		metaTopologyBuilder.setBolt("saveTypes", new SaveColumnTypesToMongoBolt(), 3)
			.shuffleGrouping("mergeTypes", "recordType");
		metaTopologyBuilder.setBolt("print", new PrintBolt())
			.shuffleGrouping("inferTypes", "recordType")
			.shuffleGrouping("mergeTypes", "recordType");
		Config conf = new Config();
		conf.setMaxSpoutPending(1000);
		conf.setNumWorkers(4);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, metaTopologyBuilder.createTopology());

		System.in.read(); //Guarda alguma coisa coisas e sai do programa

		cluster.killTopology("test");
		cluster.shutdown();
	}
}