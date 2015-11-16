package com.geofusion.importation.storm;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Semaphore;

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
	private Semaphore semaphore;
	private int numPhases;

	public TimedFlushSpout(int numPhases, long timeout) {
		this.numPhases = numPhases;
		this.timeout = timeout;
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.semaphore = new Semaphore(1);
	}

	@Override
	public void nextTuple() {
		if (semaphore.tryAcquire()) {
			try {
				Thread.sleep(timeout);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			collector.emit("flush-1", Collections.emptyList(), 1);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for (int i=0; i<=numPhases; i++) {
			declarer.declareStream("flush-"+i, new Fields());
		}
	}

	@Override
	public void ack(Object msgId) {
		int nextPhase = 1 + (Integer)msgId;
		if (nextPhase <= this.numPhases) {
			collector.emit("flush-"+nextPhase, Collections.emptyList(), nextPhase);
		} else {
			semaphore.release();
		}
	}
	@Override
	public void fail(Object msgId) {
		//Too bad :/
		//Whatever, try again later
		semaphore.release();
	}
}
