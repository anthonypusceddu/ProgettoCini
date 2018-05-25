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
    private int PreviousReplication;
    private String MedianType ;
    private OutputCollector collector;
    private int countMedian;
    private List<Incrocio> globalList;
    private TDigest globalTDIgest;

    public GlobalMedianBolt(String s, int rep) {
        this.MedianType = s;
        this.PreviousReplication = rep;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.ID, Costant.LIST_INTERSECTION,Costant.MEDIAN));


    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        countMedian = 0;
        globalTDIgest = new AVLTreeDigest(Costant.COMPRESSION);
        globalList=new ArrayList<>();
    }

    @Override
    public void execute(Tuple tuple) {
        List<Incrocio> intersections;
        TDigest globalHTDIgest;
        intersections= (List<Incrocio>) tuple.getValueByField(Costant.LIST_INTERSECTION);
        globalList.addAll(intersections);
        countMedian++;
        for(int i=0;i!= intersections.size() ;i++){
            globalTDIgest.add(intersections.get(i).getTd1());
        }
        if(countMedian >= PreviousReplication) {
            ArrayList<Incrocio> listMax = new ArrayList<Incrocio>();
            double quantil = globalTDIgest.quantile(Costant.QUANTIL);
            for ( Incrocio i : globalList) {
                if( i.getMedianaVeicoli() >= quantil ){
                    listMax.add(i);
                }
            }
            collector.emit(new Values(this.MedianType,listMax, quantil ));
            countMedian = 0;
            globalList=null;
            globalList=new ArrayList<>();
            globalTDIgest =null;
            globalTDIgest = new AVLTreeDigest(Costant.COMPRESSION);
        }
        //System.out.println("ho ricevuto tupla dal Median15MBolt");
    }

    private TDigest tDIgestWork(List<Incrocio> intersections){
        TDigest TDIgest = new AVLTreeDigest(Costant.COMPRESSION);
        for(int i=0;i!= intersections.size() ;i++){
            TDIgest.add(intersections.get(i).getTd1());
        }
        return TDIgest;

    }
}
