package com.example.demo.query2;

import com.example.demo.query1.entity.Incrocio;
import com.example.demo.costant.Costant;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IntermediateMed extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("partialmed"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        List<Incrocio> intersections= (List<Incrocio>)input.getValueByField("listaincroci");
        TDigest t = new AVLTreeDigest(Costant.COMPRESSION);
        for(int i=0;i!= intersections.size() ;i++){
            t.add(intersections.get(i).getTd1());
        }
        collector.emit(new Values(t));
    }
}