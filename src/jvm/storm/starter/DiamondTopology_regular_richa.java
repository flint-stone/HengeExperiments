package storm.starter;


import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.*;
import storm.starter.spout.RandomLogSpout;
import storm.starter.spout.TestSpout;

/**
 * Created by lexu on 1/20/17.
 */
public class DiamondTopology_regular_richa {
    public static void main(String[] args) throws Exception {

        int paralellism = 10;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout_head", new TestSpout(), paralellism).setNumTasks(10);

        builder.setBolt("bolt_1", new TestBolt(), 5).setNumTasks(paralellism ).setNumTasks(10).shuffleGrouping("spout_head");
        builder.setBolt("bolt_2", new TestBolt(), 5).setNumTasks(paralellism ).setNumTasks(10).shuffleGrouping("spout_head");
        builder.setBolt("bolt_3", new TestBolt(), 5).setNumTasks(paralellism ).setNumTasks(10).shuffleGrouping("spout_head");
        builder.setBolt("bolt_4", new TestBolt(), 5).setNumTasks(paralellism ).setNumTasks(10).shuffleGrouping("spout_head");

        BoltDeclarer output = builder.setBolt("bolt_output_3", new OutBolt(), paralellism*4).setNumTasks(40);
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

        conf.setNumWorkers(8);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                builder.createTopology());


    }

}
