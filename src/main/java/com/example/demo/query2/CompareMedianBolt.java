package com.example.demo.query2;

import com.example.demo.query1.entity.Incrocio;
import com.example.demo.costant.Costant;
import com.sun.prism.shader.AlphaTextureDifference_Color_AlphaTest_Loader;
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

public class CompareMedianBolt extends BaseRichBolt {
    //riceve la lista degli incroci e la mediana globale e ritorna la lista degli incroci
    // che hanno la mediana maggiore della mediana globale
    private OutputCollector collector;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.RESULT));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        List<Incrocio> listMax=new ArrayList<>();
        System.out.println("execute CompareMedianBolt");
        List<Incrocio> list=(ArrayList<Incrocio>) input.getValueByField(Costant.LIST_INTERSECTION);
        double median=input.getDoubleByField(Costant.MEDIAN);
        System.out.println(list);
        for (Incrocio i: list){
            if(i.getMedianaVeicoli()>=median){
                listMax.add(i);
            }
        }
        collector.emit(new Values(listMax));
        System.out.println("dimensione lista "+listMax.size());
    }
}