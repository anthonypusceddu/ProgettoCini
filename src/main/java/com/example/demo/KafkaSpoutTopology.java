package com.example.demo;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.tuple.Fields;


public class KafkaSpoutTopology {


    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    public static final String TOPIC_0 = "kafka-spout-test";

    private static final String TEST_MONGODB_URL = "mongodb://127.0.0.1:27017/prova";
    private static final String TEST_MONGODB_COLLECTION_NAME = "test";


    public static void main(String[] args) throws Exception {
        //new KafkaSpoutTopology().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {
        final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
        System.out.println("Running with broker url: " + brokerUrl);

        Config tpConf = getConfig();


        // Produttore
        Produttore p = new Produttore();
        p.inviaRecord();
        p.terminaProduttore();
        tpConf.setNumWorkers(1);
        System.setProperty("storm.jar", "/home/anthony/Scrivania/TANA/target/demo-0.0.1-SNAPSHOT-jar-with-dependencies.jar");
        StormSubmitter.submitTopology("storm-kafka-count", tpConf, getTopologyKafkaSpout(getKafkaSpoutConfig(brokerUrl)));
        tpConf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("storm-kafka-count", tpConf, getTopologyKafkaSpout(getKafkaSpoutConfig(brokerUrl)));
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
        /*tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt())
                .shuffleGrouping("kafka_spout", TOPIC_0_1_STREAM)
                .shuffleGrouping("kafka_spout", TOPIC_2_STREAM);
        tp.setBolt("kafka_bolt_1", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", TOPIC_2_STREAM);*/
       // tp.setBolt("split", new SplitSentence(), 1).shuffleGrouping("kafka_spout");
        //tp.setBolt("count", new WordCount(), 1).fieldsGrouping("split",new Fields("word"));
        tp.setBolt("mongo", new MongoInsertBolt(TEST_MONGODB_URL,TEST_MONGODB_COLLECTION_NAME,
               new SimpleMongoMapper().withFields("word", "count")), 1).fieldsGrouping("count", new Fields("word"));

        return tp.createTopology();
    }

    protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers){
        return KafkaSpoutConfig.builder(bootstrapServers,TOPIC_0)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                .setRetry(getRetryService())
                .setOffsetCommitPeriodMs(10000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

}
