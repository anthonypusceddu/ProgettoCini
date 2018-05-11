package com.example.demo.bolt;

import com.example.demo.ComparatoreIncrocio;
import com.example.demo.Incrocio;
import com.example.demo.costant.Costant;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IntermediateRank extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("classificaparziale"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        List<Incrocio> list = (List<Incrocio>)input.getValueByField("listaincroci");
        Collections.sort(list,new ComparatoreIncrocio());
        if(list.size() > Costant.TOP_K)
            list= list.subList(0, Costant.TOP_K-1);
        collector.emit(new Values(list));
    }
}