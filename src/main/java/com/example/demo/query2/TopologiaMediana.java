package com.example.demo.query2;

import com.example.demo.costant.Costant;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;


import java.util.Properties;

public class TopologiaMediana {


    public static void main(String[] args) throws Exception {
        new TopologiaMediana().runMain(args);
        ///
    }

    protected void runMain(String[] args) throws Exception {
     //   final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
      //  System.out.println("Running with broker url: " + brokerUrl);
        Config tpConf = getConfig();


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


    protected StormTopology getTopology() {
        //creazione topologia query 2
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout(Costant.SPOUT_QUERY_2, new SpoutSem(), Costant.NUM_SPOUT_QUERY_2);
        tp.setBolt(Costant.FILTER_QUERY_2, new FilterMedianBolt(), Costant.NUM_FILTER).shuffleGrouping(Costant.SPOUT_QUERY_2);
        tp.setBolt(Costant.MEDIAN15M_BOLT, new Median15MBolt().withTumblingWindow(Duration.seconds(5)), Costant.NUM_MEDIAN_15M_BOLT)
                .fieldsGrouping(Costant.FILTER_QUERY_2, new Fields(Costant.ID));
        tp.setBolt(Costant.MEDIAN1H_BOLT, new Median1HBolt()
                .withTumblingWindow(BaseWindowedBolt.Count.of(Costant.COUNT_MEDIAN_1H) ), Costant.NUM_MEDIAN_1H_BOLT)
                .shuffleGrouping(Costant.MEDIAN15M_BOLT);
        tp.setBolt(Costant.MEDIAN24H_BOLT, new Median1HBolt()
                .withTumblingWindow(BaseWindowedBolt.Count.of(Costant.COUNT_MEDIAN_24H) ), Costant.NUM_MEDIAN_24H_BOLT)
                .shuffleGrouping(Costant.MEDIAN1H_BOLT);
        tp.setBolt(Costant.GLOBAL_MEDIAN, new GlobalMedianBolt(), Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.MEDIAN15M_BOLT)
                .shuffleGrouping(Costant.MEDIAN1H_BOLT)
                .shuffleGrouping(Costant.MEDIAN24H_BOLT);

        tp.setBolt(Costant.COMPARE_BOLT, new CompareMedianBolt(), Costant.NUM_COMPARE_MEDIAN_BOLT)
                .shuffleGrouping(Costant.GLOBAL_MEDIAN);
        return tp.createTopology();
    }

}
