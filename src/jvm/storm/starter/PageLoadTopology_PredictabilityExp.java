package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.*;
import storm.starter.spout.RandomLogSpout;

public class PageLoadTopology_PredictabilityExp {
	public static void main(String[] args) throws Exception {

		int paralellism = 10; // changed from 50 to 10

		TopologyBuilder builder = new TopologyBuilder();

		
		builder.setSpout("spout_head", new RandomLogSpout(), paralellism).setNumTasks(20);;

		builder.setBolt("bolt_transform", new TransformBolt("bolt_transform"), paralellism).shuffleGrouping("spout_head").setNumTasks(20);;
		builder.setBolt("bolt_filter", new FilterBolt("bolt_filter"), 2).shuffleGrouping("bolt_transform").setNumTasks(20);;
		builder.setBolt("bolt_join", new TestBolt("bolt_join"), 2).shuffleGrouping("bolt_filter").setNumTasks(20);;
		builder.setBolt("bolt_filter_2", new FilterBolt("bolt_filter_2"), 2).shuffleGrouping("bolt_join").setNumTasks(20);;
		builder.setBolt("bolt_aggregate", new AggregationBolt("bolt_aggregate"), 2).shuffleGrouping("bolt_filter_2").setNumTasks(20);;
		builder.setBolt("bolt_output_sink", new OutBolt("sink"), 2).shuffleGrouping("bolt_aggregate").setNumTasks(20);;


		Config conf = new Config();
		conf.setTopologyLatencySlo(30.0);
		conf.setTopologySensitivity("latency");
		conf.setDebug(true);
		conf.setNumAckers(0);
		conf.setNumWorkers(5);
		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}
