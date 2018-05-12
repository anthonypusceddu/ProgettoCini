package com.example.demo.query2;

import com.example.demo.costant.Costant;
import com.example.demo.query1.entity.Produttore;
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

public class TopologiaMediana {

    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    public static final String TOPIC_0 = "classifica";
    private Properties properties;

    public static void main(String[] args) throws Exception {
        new TopologiaMediana().runMain(args);
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
        cluster.submitTopology("topologiaMediana", tpConf, getTopologyKafkaSpout(getKafkaSpoutConfig(brokerUrl)));
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
        tp.setBolt("filterBoltMed",new FilterBoltMed(),Costant.NUM_FILTER).shuffleGrouping("kafka_spout");
        tp.setBolt("MedBolt", new MedBolt().withTumblingWindow(Duration.of(Costant.WINDOW_MIN_TEST)),Costant.NUM_AVG)
                .fieldsGrouping("filterBoltMed", new Fields("id"));
        tp.setBolt("intermediateMed", new IntermediateMed(), Costant.NUM_INTERMEDIATERANK)
                .fieldsGrouping("MedBolt",new Fields("id"));

        tp.setBolt("globalMed", new GlobalMed(),1)
                .allGrouping("intermediateMed");
        tp.setBolt("calculateMax", new CalculateMax(),1)
                    .allGrouping("GlobalMed").allGrouping("MedBolt");
        return tp.createTopology();
    }

    protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers){
        this.setProperties();
        return KafkaSpoutConfig.builder(bootstrapServers,TOPIC_0)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "medianaTestGroup")
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
