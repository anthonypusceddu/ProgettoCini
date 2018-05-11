package com.example.demo;

import com.example.demo.bolt.AvgBolt;
import com.example.demo.bolt.FilterBolt;
import com.example.demo.bolt.GlobalRank;
import com.example.demo.bolt.IntermediateRank;
import com.example.demo.costant.Costant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;


import java.util.Properties;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class TopologiaClassifica {

    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    public static final String TOPIC_0 = "classifica";
    private Properties properties;

    public static void main(String[] args) throws Exception {
        new TopologiaClassifica().runMain(args);
        ///
    }

    protected void runMain(String[] args) throws Exception {
        final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
        System.out.println("Running with broker url: " + brokerUrl);

        Config tpConf = getConfig();


        // Produttore
        Produttore p = new Produttore();
        p.inviaRecord();
        p.terminaProduttore();

        // run local cluster
        tpConf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("topologiaClassifica", tpConf, getTopologyKafkaSpout(getKafkaSpoutConfig(brokerUrl)));
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
        tp.setBolt("filterBolt",new FilterBolt(),Costant.NUM_FILTER).shuffleGrouping("kafka_spout");
        tp.setBolt("avgBolt", new AvgBolt().withTumblingWindow(Duration.of(Costant.WINDOW_MIN_TEST)),Costant.NUM_AVG)
                .fieldsGrouping("filterBolt", new Fields("id"));
        tp.setBolt("intermediateRanking", new IntermediateRank(), Costant.NUM_INTERMEDIATERANK)
                .fieldsGrouping("filterBolt",new Fields("id"));
        tp.setBolt("globalRank", new GlobalRank(),1)
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
    }

}
