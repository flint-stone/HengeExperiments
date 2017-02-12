package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.OutBolt;
import storm.starter.bolt.TestBolt;
import storm.starter.spout.TestSpout;
import storm.starter.spout.TestSpout_Aggresive;

public class DiamondTopology_11_lexu {
	public static void main(String[] args) throws Exception {

		int paralellism = 1;

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout_head", new TestSpout(), paralellism*2).setNumTasks(2);

		builder.setBolt("bolt_1", new TestBolt(), paralellism).setNumTasks(paralellism ).setNumTasks(5).shuffleGrouping("spout_head");
		builder.setBolt("bolt_2", new TestBolt(), paralellism).setNumTasks(paralellism ).setNumTasks(5).shuffleGrouping("spout_head");
		builder.setBolt("bolt_3", new TestBolt(), paralellism).setNumTasks(paralellism ).setNumTasks(5).shuffleGrouping("spout_head");
		builder.setBolt("bolt_4", new TestBolt(), paralellism).setNumTasks(paralellism ).setNumTasks(5).shuffleGrouping("spout_head");

		BoltDeclarer output = builder.setBolt("bolt_output_3", new OutBolt("sink"), paralellism*1).setNumTasks(40);

		output.shuffleGrouping("bolt_1");
		output.shuffleGrouping("bolt_2");
		output.shuffleGrouping("bolt_3");
		output.shuffleGrouping("bolt_4");

		Config conf = new Config();
		//conf.setTopologySlo(0.9);
		//conf.setTopologySensitivity("throughput");
		conf.setTopologySlo(1.0);
		conf.setTopologyUtility(5);
		conf.setDebug(true);

		conf.setNumAckers(0);

		conf.setNumWorkers(2);

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());


	}


}
