package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.starter.BusyWork.BusyWork;

import java.util.Map;
import java.util.Random;

public class TestSpout_430 extends BaseRichSpout {
	  SpoutOutputCollector _collector; 
	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    _collector = collector;
	  }

	  @Override
	  public void nextTuple() {
		 BusyWork.doWork(10000);
		 Random randomGenerator = new Random();
    	 Integer randomInt = randomGenerator.nextInt(1000000000);
		  for(int i= 0;i<8;i++) {
			  _collector.emit(new Values("jerry"), randomInt);
		  }
	  }

	  @Override
	  public void ack(Object id) {
	  }

	  @Override
	  public void fail(Object id) {
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("word"));
	  }
}
