package com.geofusion.importation.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class PrintBolt extends BaseRichBolt {
    OutputCollector collector;

    @SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	this.collector = collector;
    };

    @Override
    public void execute(Tuple tuple) {
    	System.err.println("### " + tuple);
        collector.ack(tuple);
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
