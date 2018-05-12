package com.example.demo.query1.bolt;

import com.example.demo.query1.ComparatoreIncrocio;
import com.example.demo.query1.entity.Incrocio;
import com.example.demo.costant.Costant;
import com.example.demo.query1.entity.Rank;
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
    public void execute(Tuple input) {//riceve lista di incroci
        //ordina lista di incroci creando una classifica e invia classifica
        Rank rank= (Rank)input.getValueByField("listaincroci");
        List<Incrocio> list=rank.getListIntersection();
        Collections.sort(list,new ComparatoreIncrocio());
        if(list.size() > Costant.TOP_K)
            list= list.subList(0, Costant.TOP_K-1);
        rank.setListIntersection(list);
        collector.emit(new Values(rank));
    }
}