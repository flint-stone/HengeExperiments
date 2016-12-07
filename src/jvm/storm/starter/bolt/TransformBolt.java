package storm.starter.bolt;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;

import storm.starter.ExclamationTopology.ExclamationBolt;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TransformBolt extends BaseRichBolt{
	OutputCollector _collector;
    String bolt_name;
    File output_file;
    String topology_name;
    Long start_time;

    public TransformBolt() {
        bolt_name = "no-name";
    }

    public TransformBolt(String bolt_name) {
        this.bolt_name = bolt_name;
        this.start_time = System.currentTimeMillis();
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        String name = "/proj/Stella/output-bolts/output-time" + context.getStormId() + "/" + bolt_name + "/" + context.getThisTaskId() + ".log";
        output_file = new File(name);
        topology_name = context.getStormId();
      _collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {

    	//String word = tuple.getString(0);
    	//Integer length=word.length();
    	//Utils.sleep(length);
    	//word=word.substring(0,(int)(0.8*word.length()));

        String word = "useless";
        String spout = "no";
        Long time = -1l;
        if (tuple.contains("word"))
            word = tuple.getStringByField("word");
        if (tuple.contains("spout"))
            spout = tuple.getStringByField("spout");
        if (tuple.contains("time"))
            time = tuple.getLongByField("time");
        writeToFile(output_file, topology_name + "," + (System.currentTimeMillis()-time) + "," + bolt_name + "\n");


      _collector.emit(tuple, new Values(word, spout, time));//new Values(word));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "spout", "time"));
     // declarer.declare(new Fields("word"));
    }

    public void writeToFile(File file, String data) {
        try {

            FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
            FileLock lock = channel.tryLock();
            while (lock == null) {
                lock = channel.tryLock();
            }
            //FileWriter fileWriter = new FileWriter(file, true);
            FileWriter fileWriter = new FileWriter(file, false);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
            lock.release();
            channel.close();

        } catch (IOException ex) {
            System.out.println(ex.toString());

        } catch (Exception ex) {
            System.out.println(ex.toString());

        }
    }
}

