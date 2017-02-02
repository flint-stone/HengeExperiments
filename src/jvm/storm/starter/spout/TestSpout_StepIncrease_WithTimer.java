package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.starter.BusyWork.BusyWork;


import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Random;

import java.util.Map;
import java.util.Random;

public class TestSpout_StepIncrease_WithTimer extends BaseRichSpout {
	  SpoutOutputCollector _collector;
	Random _rand;
	Long time;
	Integer taskId;
	String folderName;

	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    /*_collector = collector;
		time = System.currentTimeMillis() / 1000;*/
		  _collector = collector;
		  _rand = new Random();
		  taskId = context.getThisTaskId();
		  String hostname = "Unknown";
		  folderName = "";
		  try {
			  InetAddress addr;
			  addr = InetAddress.getLocalHost();
			  hostname = addr.getHostName();
		  } catch (UnknownHostException ex) {
			  System.out.println("Hostname can not be resolved");
		  }
		  if (hostname.contains("storm-cluster-copy2.stella.emulab.net"))
			  folderName = "/proj/Stella/storm-cluster-copy2/";
		  else if (hostname.contains("storm-cluster.stella.emulab.net"))
			  folderName = "/proj/Stella/storm-cluster/";
		  else if (hostname.contains("storm-cluster-copy.stella.emulab.net"))
			  folderName = "/proj/Stella/storm-cluster-copy/";
		  else if (hostname.contains("advanced-stela.stella.emulab.net"))
			  folderName = "/proj/Stella/advanced-stela/";
		  else if (hostname.contains("stelaadvanced.stella.emulab.net"))
			  folderName = "/proj/Stella/stelaadvanced/";
		  else if (hostname.contains("hengeexperiment.stella.emulab.net"))
			  folderName = "/proj/Stella/hengeexperiment/";

		  String filename =  folderName + "start-time-" + context.getStormId() + "-" + context.getThisTaskId() + ".log";
		  File varTmpDir = new File(filename);
		  if (varTmpDir.exists() && varTmpDir.isFile()) {
			  FileReader fileReader = null;
			  try {
				  fileReader = new FileReader(filename);
				  BufferedReader bufferedReader =
						  new BufferedReader(fileReader);
				  String line = "";
				  line = bufferedReader.readLine();
				  Long temp = Long.parseLong(line);
				  time = temp;
			  } catch (Exception e) {
				  e.printStackTrace();
			  }
		  } else {
			  time = System.currentTimeMillis() / 1000;
			  try {
				  // Assume default encoding.
				  FileWriter fileWriter =
						  new FileWriter(filename);
				  BufferedWriter bufferedWriter =
						  new BufferedWriter(fileWriter);
				  bufferedWriter.write(time.toString());
				  // Always close files.
				  bufferedWriter.close();
			  } catch (IOException ex) {
				  System.out.println("Error writing to file '" + filename + "'");
			  }
		  }

        /*_collector = collector;
        _rand = new Random();
        time = System.currentTimeMillis() / 1000;
        taskId = context.getThisTaskId();*/
	  }

	  @Override
	  public void nextTuple() {
		 BusyWork.doWork(10000);
		 Random randomGenerator = new Random();
    	 Integer randomInt = randomGenerator.nextInt(1000000000);
	    //_collector.emit(new Values("jerry"), randomInt);
		  /*if (time > System.currentTimeMillis() / 1000 - 3600) {
			  //_collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
			  _collector.emit(new Values("jerry"), randomInt);
		  } else {
			  for (int i = 0; i < 3; i++) {
				 // _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
				  _collector.emit(new Values("jerry"), randomInt);
			  }
		  }*/


		  if (taskId == 61 || taskId == 68) {

			  if (time > System.currentTimeMillis() / 1000 - 1800 || time < System.currentTimeMillis() / 1000 - 12000) {
				  _collector.emit(new Values("jerry"), randomInt);
			  } else {
				  for (int i = 0; i < 2; i++) {
					  _collector.emit(new Values("jerry"), randomInt);
				  }
			  }


		  } else if (taskId == 69 || taskId == 76) {

			  if (time > System.currentTimeMillis() / 1000 - 3000 || time < System.currentTimeMillis() / 1000 - 10800) {
				  _collector.emit(new Values("jerry"), randomInt);
			  } else {
				  for (int i = 0; i < 2; i++) {
					  _collector.emit(new Values("jerry"), randomInt);
				  }
			  }

		  } else if (taskId == 77 || taskId == 84) {

			  if (time > System.currentTimeMillis() / 1000 - 4200 || time < System.currentTimeMillis() / 1000 - 9600) {
				  _collector.emit(new Values("jerry"), randomInt);
			  } else {
				  for (int i = 0; i < 2; i++) {
					  _collector.emit(new Values("jerry"), randomInt);
				  }
			  }

		  } else if (taskId == 85 || taskId == 92) {

			  if (time > System.currentTimeMillis() / 1000 - 5400 || time < System.currentTimeMillis() / 1000 - 8400) {
				  _collector.emit(new Values("jerry"), randomInt);
			  } else {
				  for (int i = 0; i < 2; i++) {
					  _collector.emit(new Values("jerry"), randomInt);
				  }
			  }

		  } else if (taskId == 93 || taskId == 100) {

			  if (time > System.currentTimeMillis() / 1000 - 6600 || time < System.currentTimeMillis() / 1000 - 7200) {
				  _collector.emit(new Values("jerry"), randomInt);
			  } else {
				  for (int i = 0; i < 2; i++) {
					  _collector.emit(new Values("jerry"), randomInt);
				  }
			  }
		  } else {
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
