package com.example.demo.control;


import com.example.demo.control.bolt.FilterControlBolt;
import com.example.demo.control.bolt.SumBolt;
import com.example.demo.control.bolt.WebsterBolt;
import com.example.demo.spout.SpoutTraffic;
import com.example.demo.costant.Costant;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;

public class TopologyControl {


    public static void main(String[] args) throws Exception {
        new TopologyControl().runMain(args);
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
        tp.setSpout(Costant.SPOUT, new SpoutTraffic(), Costant.NUM_SPOUT_CONTROL);


        tp.setBolt(Costant.FILTER_CONTROL, new FilterControlBolt(), Costant.NUM_FILTER_CONTROL).shuffleGrouping(Costant.SPOUT);
        tp.setBolt(Costant.SUM_BOLT, new SumBolt().withTumblingWindow(Duration.hours(Costant.WINDOW_HOUR)), Costant.NUM_SUM_BOLT)
                .fieldsGrouping(Costant.FILTER_CONTROL, new Fields(Costant.ID));
        tp.setBolt(Costant.WEBSTER_BOLT, new WebsterBolt(), Costant.NUM_WEBSTER_BOLT)
                .shuffleGrouping(Costant.SUM_BOLT);
        return tp.createTopology();
    }

}
