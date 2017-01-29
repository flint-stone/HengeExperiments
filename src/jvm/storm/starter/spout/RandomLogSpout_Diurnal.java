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

public class RandomLogSpout_Diurnal extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    Long time;
    Integer taskId;
    String folderName;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
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
        Utils.sleep(50);
        String[] sentences = new String[]{"the cow jumped over the moon", "the cow jumped over the moon, The quick brown fox jumps over the lazy dog", "an apple a day keeps the doctor away,an apple a day keeps the doctor away and this is supposed to be a very long log line",
                "four score and seven years ago", "random", "snow white and the seven dwarfs", "snow white", "i am at two with nature"};
        String sentence = sentences[_rand.nextInt(sentences.length)];
        if (taskId == 901 || taskId == 906) {

            if (time > System.currentTimeMillis() / 1000 - 4000 || time < System.currentTimeMillis() / 1000 - 13000) {
                _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
            } else {
                for (int i = 0; i < 2; i++) {
                    _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
                }
            }


        } else if (taskId == 902 || taskId == 907) {

            if (time > System.currentTimeMillis() / 1000 - 5000 || time < System.currentTimeMillis() / 1000 - 13000) {
                _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
            } else {
                for (int i = 0; i < 2; i++) {
                    _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
                }
            }

        } else if (taskId == 903 || taskId == 908) {

            if (time > System.currentTimeMillis() / 1000 - 6000 || time < System.currentTimeMillis() / 1000 - 12000) {
                _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
            } else {
                for (int i = 0; i < 2; i++) {
                    _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
                }
            }

        } else if (taskId == 904 || taskId == 909) {

            if (time > System.currentTimeMillis() / 1000 - 7000 || time < System.currentTimeMillis() / 1000 - 11000) {
                _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
            } else {
                for (int i = 0; i < 2; i++) {
                    _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
                }
            }

        } else if (taskId == 905 || taskId == 910) {

            if (time > System.currentTimeMillis() / 1000 - 8000 || time < System.currentTimeMillis() / 1000 - 10000) {
                _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));/**/
            } else {
                for (int i = 0; i < 2; i++) {
                    _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
                }
            }
        } else {
            _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
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
        declarer.declare(new Fields("word", "spout", "time"));
    }

}
