package com.example.demo.query2;


import com.example.demo.costant.Costant;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class GlobalMed extends BaseRichBolt {
    private OutputCollector collector;
    private int countIntermediateMed;
    private TDigest t;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        countIntermediateMed = 0;
        t= new AVLTreeDigest(Costant.COMPRESSION);
    }

    @Override
    public void execute(Tuple tuple) {
        countIntermediateMed++;
        t.add((TDigest)tuple.getValueByField("partialmed"));
        System.out.println("cont"+countIntermediateMed);

        if(countIntermediateMed >= Costant.NUM_INTERMEDIATERANK) {
            System.out.println("emit globalmed");
            collector.emit(new Values(t.quantile(Costant.QUANTIL)));
            t = null;
            t = new AVLTreeDigest(Costant.COMPRESSION);
            countIntermediateMed = 0;
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("totalmed"));

    }
}
