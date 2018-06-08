package com.example.demo.query2.bolt;


import com.example.demo.costant.Costant;
import com.example.demo.entity.Intersection;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class GlobalMedianBolt extends BaseRichBolt {
    private int PreviousReplication;
    private String MedianType ;
    private OutputCollector collector;
    private int countMedian;
    private List<Intersection> globalList;
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
        List<Intersection> intersections;
        intersections= (List<Intersection>) tuple.getValueByField(Costant.LIST_INTERSECTION);
        globalList.addAll(intersections);
        countMedian++;
        for(int i=0;i!= intersections.size() ;i++){
            globalTDIgest.add(intersections.get(i).getTd1());
        }
        if(countMedian >= PreviousReplication) {
            ArrayList<Intersection> listMax = new ArrayList<>();
            double quantil = globalTDIgest.quantile(Costant.QUANTIL);
            for ( Intersection i : globalList) {
                if( i.getMedianaVeicoli() >= quantil ){
                    listMax.add(i);
                }
            }
          //  collector.emit(new Values(this.MedianType,listMax, quantil ));
            System.out.println(this.MedianType +"   " + quantil + "   "+listMax.size());
            countMedian = 0;
            globalList=null;
            globalList=new ArrayList<>();
            globalTDIgest =null;
            globalTDIgest = new AVLTreeDigest(Costant.COMPRESSION);
        }
    }
}
