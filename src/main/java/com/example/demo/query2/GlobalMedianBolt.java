package com.example.demo.query2;


import com.example.demo.costant.Costant;
import com.example.demo.query1.entity.Incrocio;
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

public class GlobalMedianBolt extends BaseRichBolt {
    //GlobalMedianBolt riceve tutte liste di incroci e
    // le mette insieme per creare un unica lista che contiene tutti gli incroci
    //dalla lista contentente tutti gli incroci calcola la mediana globale
    // e manda tutta la lista e la mediana global al calculatemax
    private OutputCollector collector;
    private int countMedianBolt;
    private List<Incrocio> globalList;
    private TDigest globalTDIgest;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        countMedianBolt = 0;
        globalTDIgest= new AVLTreeDigest(Costant.COMPRESSION);
        globalList=new ArrayList<>();
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("inizio execute global med");
        countMedianBolt++;
        List<Incrocio> intersections= (List<Incrocio>)tuple.getValueByField(Costant.LIST_INTERSECTION);
        globalList.addAll(intersections);
        for(int i=0;i!= intersections.size() ;i++){
            globalTDIgest.add(intersections.get(i).getTd1());
        }
        if(countMedianBolt >= Costant.NUM_MEDIAN_BOLT) {
            System.out.println("emit globalmed"+globalList);
            collector.emit(new Values(globalList,globalTDIgest.quantile(Costant.QUANTIL)));
            countMedianBolt = 0;
            globalList.clear();
            globalTDIgest=null;
            globalTDIgest = new AVLTreeDigest(Costant.COMPRESSION);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.LIST_INTERSECTION,Costant.MEDIAN));

    }
}
