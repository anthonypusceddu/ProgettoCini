package com.example.demo.query1;

import com.example.demo.query1.bolt.AvgBolt;
import com.example.demo.query1.bolt.FilterBolt;
import com.example.demo.query1.bolt.GlobalRankBolt;
import com.example.demo.query1.bolt.IntermediateRankBolt;
import com.example.demo.costant.Costant;
import com.example.demo.spout.SpoutTraffic;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;


import java.util.Properties;

public class Topology1 {

    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    public static final String TOPIC_0 = "classifica";
    private Properties properties;

    public static void main(String[] args) throws Exception {
        new Topology1().runMain(args);
        ///
    }

    protected void runMain(String[] args) throws Exception {
        Config tpConf = getConfig();


        /*// Produttore
        Produttore p = new Produttore();
        p.inviaRecord();
        p.terminaProduttore();*/

        // run local cluster
        tpConf.setMaxTaskParallelism(Costant.NUM_PARALLELISM);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Costant.TOPOLOGY_QUERY_1, tpConf, getTopology());
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(false);
        config.setMessageTimeoutSecs(Costant.MESSAGE_TIMEOUT_SEC);
        return config;
    }

    protected StormTopology getTopology() {
        final TopologyBuilder tp = new TopologyBuilder();

        tp.setSpout(Costant.SPOUT, new SpoutTraffic(), Costant.NUM_SPOUT_QUERY_1);

        tp.setBolt(Costant.FILTER_QUERY_1,new FilterBolt(),Costant.NUM_FILTER_QUERY1).shuffleGrouping(Costant.SPOUT);

        tp.setBolt(Costant.AVG15M_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(5)),Costant.NUM_AVG15M)
                .fieldsGrouping(Costant.FILTER_QUERY_1, new Fields(Costant.ID));

        tp.setBolt(Costant.AVG1H_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(10)),Costant.NUM_AVG1H)
                .fieldsGrouping(Costant.FILTER_QUERY_1, new Fields(Costant.ID));

        tp.setBolt(Costant.AVG24H_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(15)),Costant.NUM_AVG24H)
                .fieldsGrouping(Costant.FILTER_QUERY_1, new Fields(Costant.ID));

        tp.setBolt(Costant.INTERMEDIATERANK_15M, new IntermediateRankBolt(), Costant.NUM_INTERMEDIATERANK15M)
                .shuffleGrouping(Costant.AVG15M_BOLT);

        tp.setBolt(Costant.INTERMEDIATERANK_1H, new IntermediateRankBolt(), Costant.NUM_INTERMEDIATERANK1H)
                .shuffleGrouping(Costant.AVG1H_BOLT);

        tp.setBolt(Costant.INTERMEDIATERANK_24H, new IntermediateRankBolt(), Costant.NUM_INTERMEDIATERANK24H)
                .shuffleGrouping(Costant.AVG24H_BOLT);

        tp.setBolt(Costant.GLOBAL15M_AVG, new GlobalRankBolt(Costant.ID15M,Costant.NUM_AVG15M),Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.INTERMEDIATERANK_15M);

        tp.setBolt(Costant.GLOBAL1H_AVG, new GlobalRankBolt(Costant.ID1H,Costant.NUM_AVG1H),Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.INTERMEDIATERANK_1H);

        tp.setBolt(Costant.GLOBAL24H_AVG, new GlobalRankBolt(Costant.ID24H,Costant.NUM_AVG24H),Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.INTERMEDIATERANK_24H);
        return tp.createTopology();
    }

    /*protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), Costant.NUM_SPOUT_QUERY_1);
        tp.setBolt("filterBolt",new FilterBolt(),Costant.NUM_FILTER).shuffleGrouping("kafka_spout");
        tp.setBolt("avgBolt", new AvgBolt().withWindow(Duration.minutes(Costant.WINDOW_MIN),Duration.of(Costant.SEC_TUPLE)),Costant.NUM_AVG)
                .fieldsGrouping("filterBolt", new Fields("id"));
        tp.setBolt("intermediateRanking", new IntermediateRankBolt(), Costant.NUM_INTERMEDIATERANK)
                .fieldsGrouping("avgBolt",new Fields("id"));
        tp.setBolt("globalRank", new GlobalRankBolt(),1)
                .allGrouping("intermediateRanking");
        return tp.createTopology();
    }

    protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers){
        this.setProperties();
        return KafkaSpoutConfig.builder(bootstrapServers,TOPIC_0)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "classificaTestGroup")
                .setProp(properties)
                .setRetry(getRetryService())
                .setOffsetCommitPeriodMs(10000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected void setProperties() {
        properties = new Properties();
        properties.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
    }
    protected KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }*/

}
