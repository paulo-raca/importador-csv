package com.geofusion.importation.storm;

import java.util.Collections;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * Envia uma mensagem a cada X millisegundos avisando a todos os spouts que consolidem as agregações atuais
 */
@SuppressWarnings("serial")
public class TimedFlushSpout extends BaseRichSpout {
	long timeout;
	private SpoutOutputCollector collector;
	
	public TimedFlushSpout(long timeout) {
		this.timeout = timeout;
	}
	
    @SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }
    
	@Override
	public void nextTuple() {
		try {
			Thread.sleep(timeout);
			collector.emit(Collections.emptyList());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields());
	}
}
