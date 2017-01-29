package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.*;
import storm.starter.spout.RandomLogSpout;
import storm.starter.spout.RandomLogSpout_Diurnal;

public class PageLoadTopology_Lexu_Diurnal {
	public static void main(String[] args) throws Exception {

		int paralellism = 10; // changed from 50 to 10

		TopologyBuilder builder = new TopologyBuilder();

		
		builder.setSpout("spout_head", new RandomLogSpout_Diurnal(), paralellism).setNumTasks(paralellism);

		builder.setBolt("bolt_transform", new TransformBolt("bolt_transform"), paralellism).shuffleGrouping("spout_head").setNumTasks(150);
		builder.setBolt("bolt_filter", new FilterBolt("bolt_filter"), paralellism).shuffleGrouping("bolt_transform").setNumTasks(150);
		builder.setBolt("bolt_join", new TestBolt("bolt_join"), paralellism).shuffleGrouping("bolt_filter").setNumTasks(150);
		builder.setBolt("bolt_filter_2", new FilterBolt("bolt_filter_2"), paralellism).shuffleGrouping("bolt_join").setNumTasks(150);
		builder.setBolt("bolt_aggregate", new AggregationBolt("bolt_aggregate"), 4).shuffleGrouping("bolt_filter_2").setNumTasks(150);
		builder.setBolt("bolt_output_sink", new OutBolt("sink"),paralellism).shuffleGrouping("bolt_aggregate").setNumTasks(150);


		Config conf = new Config();
		//conf.setTopologySlo(1.0);
		conf.setTopologySlo(0.00001);
		conf.setTopologyLatencySlo(60.0);
		conf.setTopologyUtility(35);
		conf.setDebug(true);
		conf.setNumAckers(0);
		conf.setNumWorkers(2);
		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}
