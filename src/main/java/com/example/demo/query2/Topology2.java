package com.example.demo.query2;

import com.example.demo.query2.bolt.FilterMedianBolt;
import com.example.demo.query2.bolt.GlobalMedianBolt;
import com.example.demo.query2.bolt.MedianBolt;
import com.example.demo.spout.SpoutTraffic;
import com.example.demo.costant.Costant;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;

public class Topology2 {


    public static void main(String[] args) throws Exception {
        new Topology2().runMain(args);
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
        tp.setSpout(Costant.SPOUT, new SpoutTraffic(), Costant.NUM_SPOUT_QUERY_2);

        tp.setBolt(Costant.FILTER_QUERY_2, new FilterMedianBolt(), Costant.NUM_FILTER_QUERY2).shuffleGrouping(Costant.SPOUT);

        tp.setBolt(Costant.MEDIAN15M_BOLT, new MedianBolt().withTumblingWindow(Duration.minutes(Costant.WINDOW_MIN)), Costant.NUM_MEDIAN_15M_BOLT)
                .fieldsGrouping(Costant.FILTER_QUERY_2, new Fields(Costant.ID));

        tp.setBolt(Costant.MEDIAN1H_BOLT, new MedianBolt().withTumblingWindow(Duration.hours(Costant.WINDOW_HOUR)), Costant.NUM_MEDIAN_1H_BOLT)
                .fieldsGrouping(Costant.FILTER_QUERY_2, new Fields(Costant.ID));

        tp.setBolt(Costant.MEDIAN24H_BOLT, new MedianBolt().withTumblingWindow(Duration.days(Costant.WINDOW_DAY)), Costant.NUM_MEDIAN_24H_BOLT)
                .fieldsGrouping(Costant.FILTER_QUERY_2, new Fields(Costant.ID));

       tp.setBolt(Costant.GLOBAL15M_MEDIAN, new GlobalMedianBolt(Costant.ID15M, Costant.NUM_MEDIAN_15M_BOLT), Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.MEDIAN15M_BOLT);

        tp.setBolt(Costant.GLOBAL1H_MEDIAN, new GlobalMedianBolt(Costant.ID1H, Costant.NUM_MEDIAN_1H_BOLT), Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.MEDIAN1H_BOLT);

        tp.setBolt(Costant.GLOBAL24H_MEDIAN, new GlobalMedianBolt(Costant.ID24H, Costant.NUM_MEDIAN_24H_BOLT), Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.MEDIAN24H_BOLT);

        return tp.createTopology();
    }

}
