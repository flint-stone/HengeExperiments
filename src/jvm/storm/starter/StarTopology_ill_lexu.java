package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.*;
import storm.starter.spout.RandomLogSpout;
import storm.starter.spout.TestSpout;

public class StarTopology_ill_lexu {
	public static void main(String[] args) throws Exception {

		//int numSpout = 4;
		//int numBolt = 4;
		int numSpout=4;
		int numBolt=4;
		int paralellism = 40;

		TopologyBuilder builder = new TopologyBuilder();

		BoltDeclarer center = builder.setBolt("center", new TestBolt(),
				 paralellism*2).setNumTasks(160);

		for (int i = 0; i < numSpout; i++) {
			builder.setSpout("spout_" + i, new RandomLogSpout(), paralellism*2).setNumTasks(160);
			center.shuffleGrouping("spout_" + i);
		}

		for (int i = 0; i < numBolt; i++) {
			builder.setBolt("bolt_output_" + i, new OutBolt("sink"), paralellism*2).setNumTasks(160)
					.shuffleGrouping("center");
		}


		Config conf = new Config();
		//conf.setTopologySlo(0.9);
		//conf.setTopologySensitivity("throughput");
		conf.setTopologySlo(1.0);
		conf.setTopologyUtility(5);
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_DEBUG, false);

		conf.setNumAckers(0);

		conf.setNumWorkers(8);



		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}
