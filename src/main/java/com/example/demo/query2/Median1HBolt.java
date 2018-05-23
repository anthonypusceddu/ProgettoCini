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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Median1HBolt extends BaseRichBolt {

    private int count;//max 4
    private OutputCollector collector;
    private TDigest t;
    private ArrayList<Incrocio> globalList;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.LIST_INTERSECTION));

    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        count=0;
        t=new AVLTreeDigest(Costant.COMPRESSION);
        globalList=new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        count++;
        ArrayList<Incrocio>list=(ArrayList)input.getValueByField(Costant.LIST_INTERSECTION);
        //unisci NUM_median15mbolt in un unica lista incroci
        //ai successivi NUM_median15mbolt si uniscono le 2 liste
        if(count%Costant.NUM_MEDIAN_15M_BOLT!=0) {
            globalList.addAll(list);
        }
        else{

        }
        if(count==Costant.COUNT_MEDIAN_1H*Costant.NUM_MEDIAN_15M_BOLT){
            count=0;
            t=null;
            t=new AVLTreeDigest(Costant.COMPRESSION);
            collector.emit(new Values( new ArrayList<Incrocio>()));
        }
        System.out.println("ho inviato dal Median1HBolt");
    }


}
