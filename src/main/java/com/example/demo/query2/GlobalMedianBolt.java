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
import org.apache.storm.utils.Utils;

import java.util.*;

public class GlobalMedianBolt extends BaseRichBolt {
    //GlobalMedianBolt riceve tutte liste di incroci e
    // le mette insieme per creare un unica lista che contiene tutti gli incroci
    //dalla lista contentente tutti gli incroci calcola la mediana globale
    // e manda tutta la lista e la mediana global al calculatemax
    private OutputCollector collector;
    private int countMedian15MBolt;
    private List<Incrocio> global15MList;
    private TDigest global15MTDIgest;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.ID, Costant.LIST_INTERSECTION,Costant.MEDIAN));


    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        countMedian15MBolt = 0;
        global15MTDIgest= new AVLTreeDigest(Costant.COMPRESSION);
        global15MList=new ArrayList<>();
    }

    @Override
    public void execute(Tuple tuple) {
        List<Incrocio> intersections;
        TDigest globalHTDIgest;

        tuple.getSourceComponent();

        // 15 Minuti
        if (   tuple.getSourceComponent().equals(Costant.MEDIAN15M_BOLT) ){
            intersections= (List<Incrocio>) tuple.getValueByField(Costant.LIST_INTERSECTION);
            countMedian15MBolt++;
            global15MList.addAll(intersections);
            for(int i=0;i!= intersections.size() ;i++){
                global15MTDIgest.add(intersections.get(i).getTd1());
            }
            if(countMedian15MBolt >= Costant.NUM_MEDIAN_15M_BOLT) {
               // System.out.println("emit globalmed"+global15MList);
               // System.out.println(global15MList.get(0));
                collector.emit(new Values(Costant.ID15M,global15MList,global15MTDIgest.quantile(Costant.QUANTIL)));
                countMedian15MBolt = 0;
                global15MList=null;
                global15MList=new ArrayList<>();
                global15MTDIgest=null;
                global15MTDIgest = new AVLTreeDigest(Costant.COMPRESSION);
            }
            //System.out.println("ho ricevuto tupla dal Median15MBolt");
        }

        //1 ora
        if ( tuple.getSourceComponent().equals(Costant.MEDIAN1H_BOLT) ){
            intersections=  (List<Incrocio>) tuple.getValueByField(Costant.LIST_INTERSECTION);
            globalHTDIgest= tDIgestWork(intersections);
            collector.emit(new Values(Costant.ID1H,intersections,globalHTDIgest.quantile(Costant.QUANTIL)));
            //System.out.println("ho ricevuto tupla dal Median1HBolt");
        }

        //24 ore
        if (  tuple.getSourceComponent().equals(Costant.MEDIAN24H_BOLT) ){
            intersections=  (List<Incrocio>) tuple.getValueByField(Costant.LIST_INTERSECTION);
            globalHTDIgest= tDIgestWork(intersections);
            collector.emit(new Values(Costant.ID24H,intersections,globalHTDIgest.quantile(Costant.QUANTIL)));
            //System.out.println("ho ricevuto tupla dal Median24HBolt");
        }
    }





    private TDigest tDIgestWork(List<Incrocio> intersections){
        TDigest TDIgest = new AVLTreeDigest(Costant.COMPRESSION);
        for(int i=0;i!= intersections.size() ;i++){
            TDIgest.add(intersections.get(i).getTd1());
        }
        return TDIgest;

    }
}
