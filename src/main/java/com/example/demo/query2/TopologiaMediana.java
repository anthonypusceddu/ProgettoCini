package com.example.demo.query2;

import com.example.demo.costant.Costant;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;


import java.util.Properties;

public class TopologiaMediana {

    //private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    //public static final String TOPIC_0 = "classifica";// dare nuovo nome per topic e mettere topic_1
    private Properties properties;

    public static void main(String[] args) throws Exception {
        new TopologiaMediana().runMain(args);
        ///
    }

    protected void runMain(String[] args) throws Exception {
     //   final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
      //  System.out.println("Running with broker url: " + brokerUrl);
        Config tpConf = getConfig();

        /*// Produttore
        Produttore p = new Produttore();
        p.inviaRecord();
        p.terminaProduttore();*/

        // run local cluster
        tpConf.setMaxTaskParallelism(Costant.NUM_PARALLELISM);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Costant.TOPOLOGY_QUERY_2, tpConf, getTopology());//topologia query 2
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(false);
        config.setMessageTimeoutSecs(Costant.MESSAGE_TIMEOUT_SEC);
        return config;
    }

   /* protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), Costant.NUM_SPOUT_QUERY_1);
        tp.setBolt("filterBoltMed",new FilterMedianBolt(),Costant.NUM_FILTER).shuffleGrouping("sem");
        tp.setBolt("MedianBolt", new MedianBolt().withTumblingWindow(Duration.of(Costant.WINDOW_MIN_TEST)),Costant.NUM_AVG)
                .fieldsGrouping("filterBoltMed", new Fields("id"));
        tp.setBolt("intermediateMed", new IntermediateMed(), Costant.NUM_INTERMEDIATERANK)
                .fieldsGrouping("MedianBolt",new Fields("id"));

        tp.setBolt("globalMed", new GlobalMedianBolt(),1)
                .allGrouping("intermediateMed");
        tp.setBolt("calculateMax", new CompareMedianBolt(),1)
                    .allGrouping("GlobalMedianBolt").allGrouping("MedianBolt");
        return tp.createTopology();
    }*/


    protected StormTopology getTopology() {
        //creazione topologia query 2
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout(Costant.SPOUT_QUERY_2, new SpoutSem(), Costant.NUM_SPOUT_QUERY_2);
        tp.setBolt(Costant.FILTER_QUERY_2, new FilterMedianBolt(), Costant.NUM_FILTER).shuffleGrouping(Costant.SPOUT_QUERY_2);
        tp.setBolt(Costant.MEDIAN_BOLT, new MedianBolt().withTumblingWindow(Duration.seconds(5)), Costant.NUM_MEDIAN_BOLT)
                .fieldsGrouping(Costant.FILTER_QUERY_2, new Fields(Costant.ID));
        //tp.setBolt(Costant.MEDIAN_BOLT, new MedianBolt().withTumblingWindow(Duration.minutes(Costant.WINDOW_MIN)), Costant.NUM_MEDIAN_BOLT)
          //      .fieldsGrouping(Costant.FILTER_QUERY_2, new Fields(Costant.ID));

        tp.setBolt(Costant.GLOBAL_MEDIAN, new GlobalMedianBolt(), Costant.NUM_GLOBAL_BOLT)
               .shuffleGrouping(Costant.MEDIAN_BOLT);
        tp.setBolt(Costant.COMPARE_BOLT, new CompareMedianBolt(), Costant.NUM_COMPARE_MEDIAN_BOLT)
                .shuffleGrouping(Costant.GLOBAL_MEDIAN);
        return tp.createTopology();
    }

    /*protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers){
        this.setProperties();
        return KafkaSpoutConfig.builder(bootstrapServers,Costant.TOPIC_0)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "medianaTestGroup")
                .setProp(properties)
                .setRetry(getRetryService())
                .setOffsetCommitPeriodMs(10000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }*/

    /*protected void setProperties() {
        properties = new Properties();
        properties.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
    }
    protected KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }*/

}
