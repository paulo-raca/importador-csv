package com.geofusion.importation.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class PrintBolt extends BaseBasicBolt {
	OutputCollector collector;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.err.println("### " + input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
