package com.geofusion.importation.storm;

import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class ImportadorStorm {	
	public static void main(String[] args) throws IOException {
		TopologyBuilder metaTopologyBuilder = new TopologyBuilder();        

		metaTopologyBuilder.setSpout("source", new MultiFileParserSpout("storm", "import_meta"));
		metaTopologyBuilder.setBolt("inferTypes", new ColumnTypesInferenceBolt(), 3)
	    	.shuffleGrouping("source", "records")
	    	.allGrouping("source", "flush");
		metaTopologyBuilder.setBolt("print", new PrintBolt(), 3)
        	.shuffleGrouping("source", "flush");

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