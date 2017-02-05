/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Random;

public class RandomLogSpout_FB2 extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    String folderName;
    Long time;




    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();

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
        else if (hostname.contains("stelaadvanced2.stella.emulab.net"))
            folderName = "/proj/Stella/stelaadvanced2/";

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

    }

    @Override
    public void nextTuple() {
        Utils.sleep(50);
        String[] sentences = new String[]{"the cow jumped over the moon", "the cow jumped over the moon, The quick brown fox jumps over the lazy dog", "an apple a day keeps the doctor away,an apple a day keeps the doctor away and this is supposed to be a very long log line",
                "four score and seven years ago", "random", "snow white and the seven dwarfs", "snow white", "i am at two with nature"};
        String sentence = sentences[_rand.nextInt(sentences.length)];

        int intervalStart = 0;
        int intervalEnd = 600;
        int intervalIdx = 0;
        int[] p = {581,438,609,607,429,542,796,1389,1784,1871,2003,2185,1838,2287,1934,1637,1527,1070,982,673,858,681,659,531,
                581,438,609,607,429,542,796,1389,1784,1871,2003,2185,1838,2287,1934,1637,1527,1070,982,673,858,681,659,531};
        while(intervalIdx<48){

            if ( (time > System.currentTimeMillis() / 1000 - intervalEnd )&& (time < System.currentTimeMillis() / 1000 - intervalStart)) {
                for(int i = 0; i<5; i++){
                    //http://stackoverflow.com/questions/9724404/random-floating-point-double-in-inclusive-range
                    /*double dice = Math.random() < 0.5 ? (1-Math.random()):Math.random();
                    if(dice<=(p[i]/(double)2287)){
                        _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));
                    }*/
                    double dice = Math.random();
                    if(dice<(p[intervalIdx]/(double)2287)){
                        _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));
                    }
                }
                return;
            } else {
                intervalStart+=600;
                intervalEnd+=600;
                intervalIdx++;
            }

        }
        _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));
        return;

    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "spout", "time"));
    }


}
