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

public class TestSpout_WithTimer extends BaseRichSpout {
	  SpoutOutputCollector _collector;
	  Long time;

	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    _collector = collector;
		time = System.currentTimeMillis() / 1000;
	  }

	  @Override
	  public void nextTuple() {
		 BusyWork.doWork(10000);
		 Random randomGenerator = new Random();
    	 Integer randomInt = randomGenerator.nextInt(1000000000);
	    //_collector.emit(new Values("jerry"), randomInt);
		  if (time > System.currentTimeMillis() / 1000 - 3600) {
			  //_collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
			  _collector.emit(new Values("jerry"), randomInt);
		  } else {
			  for (int i = 0; i < 3; i++) {
				 // _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
				  _collector.emit(new Values("jerry"), randomInt);
			  }
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
