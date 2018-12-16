package com.kafka.storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class MyKafkaBolt implements IBasicBolt {

    /**
     *
     */
    private static final long serialVersionUID = 1L;


    public void cleanup() {
        // TODO Auto-generated method stub

    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        // TODO Auto-generated method stub

        String kafkaMsg = input.getString(0) ;
        System.err.println("bolt:"+kafkaMsg);
        System.err.println("bolt value : "+input.toString());

        // [smartdata-topic, 0, 697070, key, 502000001020532|8677090369657900|15342127055|8|1544767129983|1544767129983|8|100.72.127.8|35977219]
        String a = input.getValues().toString();
        // b : [topic, partition, offset, key, value]
        String b = input.getFields().toString();
        System.err.println("bolt a : "+a);
        System.err.println("bolt b : "+ b);

        String c = input.getStringByField("value");
        System.err.println("bolt kafka record c : "+ c);


    }

    public void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
