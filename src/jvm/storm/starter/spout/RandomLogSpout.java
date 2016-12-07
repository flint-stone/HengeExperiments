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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class RandomLogSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    Long time;


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
     /*   String filename = "/proj/Stella/storm-cluster/start-time-" + context.getStormId() + "-" + context.getThisTaskId() + ".log";
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
        }*/
    }

    @Override
    public void nextTuple() {
        Utils.sleep(50);
        String[] sentences = new String[]{"the cow jumped over the moon", "the cow jumped over the moon, The quick brown fox jumps over the lazy dog", "an apple a day keeps the doctor away,an apple a day keeps the doctor away and this is supposed to be a very long log line",
                "four score and seven years ago", "random", "snow white and the seven dwarfs", "snow white", "i am at two with nature"};
        String sentence = sentences[_rand.nextInt(sentences.length)];
        // Values v = new Values();
        // v.add(sentence);

        // HashMap values = new HashMap();
        // values.put("sentence", sentence);
        // values.put("spout", "spout_head");
        // values.put("start-time", System.currentTimeMillis());
        // v.add(values);
        _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
        /*if (time > System.currentTimeMillis() / 1000 - 35000) {
            _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
        } else {
            for (int i = 0; i < 15; i++) {
                _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
                _collector.emit(new Values(sentence, "spout_head", System.currentTimeMillis()));//new Values(sentence));
            }
        }*/
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
