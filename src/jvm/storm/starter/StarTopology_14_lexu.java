package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.OutBolt;
import storm.starter.bolt.TestBolt;
import storm.starter.spout.RandomLogSpout;

public class StarTopology_14_lexu {
	public static void main(String[] args) throws Exception {

		//int numSpout = 4;
		//int numBolt = 4;
		int numSpout=4;
		int numBolt=4;
		int paralellism = 10;

		TopologyBuilder builder = new TopologyBuilder();

		BoltDeclarer center = builder.setBolt("center", new TestBolt(),
				1).setNumTasks(20);

		for (int i = 0; i < numSpout; i++) {
			builder.setSpout("spout_" + i, new RandomLogSpout(), paralellism).setNumTasks(20);
			center.shuffleGrouping("spout_" + i);
		}

		for (int i = 0; i < numBolt; i++) {
			builder.setBolt("bolt_output_" + i, new OutBolt("sink"), paralellism).setNumTasks(20)
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

		conf.setNumWorkers(2);



		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}
