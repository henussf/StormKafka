package com.kafka.storm;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

public class MykafkaSpout {

    public static void main(String[] args) throws AuthorizationException {
        // TODO Auto-generated method stub

        String topic = "smartdata-topic" ;
        ZkHosts zkHosts = new ZkHosts("10.154.0.163:2181");
//        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic,
//                "ssf",
//                "MyTrack") ;
        KafkaSpoutConfig spoutConfig = configureKafkaSpout();
        List<String> zkServers = new ArrayList<String>() ;
        zkServers.add("10.154.0.163");
//        spoutConfig.zkServers = zkServers;
//        spoutConfig.zkPort = 2181;
//        spoutConfig.socketTimeoutMs = 60 * 1000 ;
//        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()) ;
//        spoutConfig

        TopologyBuilder builder = new TopologyBuilder() ;
        builder.setSpout("spout", new KafkaSpout(spoutConfig) ,1) ;
        builder.setBolt("bolt1", new MyKafkaBolt(), 1).shuffleGrouping("spout") ;
//        builder.setBolt("bolt2", new PrintBolt(), 1).shuffleGrouping("spout2") ;

        Config conf = new Config() ;
        conf.setDebug(false) ;

//        Config conf = configureStorm();

        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder.createTopology());
        }

    }

    public static Config configureStorm() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
        return config;
    }

    public static KafkaSpoutConfig<String, String> configureKafkaSpout() {

        KafkaSpoutConfig<String, String> kafkaConf = KafkaSpoutConfig
                .builder("10.154.0.163:9092","smartdata-topic")
                .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .setGroupId("ssfWho")
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                .setEmitNullTuples(false)
                .build();

        return kafkaConf;
    }

}
