package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.OutBolt;
import storm.starter.bolt.TestBolt;
import storm.starter.spout.RandomLogSpout;

public class LinearTopology_1_lexu {
	public static void main(String[] args) throws Exception {

		int numBolt = 3;
		int paralellism = 5;

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout_head", new RandomLogSpout(), paralellism*4).setNumTasks(80);

		for (int i = 0; i < numBolt; i++) {
			if (i == 0) {
				builder.setBolt("bolt_linear_" + i, new TestBolt(), paralellism*4).setNumTasks(80)
						.shuffleGrouping("spout_head");
			} else {
				if (i == (numBolt - 1)) {
					builder.setBolt("bolt_output_" + i, new OutBolt("sink"),
							paralellism*4).setNumTasks(80).shuffleGrouping(
							"bolt_linear_" + (i - 1));
				} else {
					builder.setBolt("bolt_linear_" + i, new TestBolt(),
							paralellism*4).setNumTasks(80).shuffleGrouping(
							"bolt_linear_" + (i - 1));
				}
			}
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