package com.example.demo.query2;

import com.example.demo.query1.entity.Incrocio;
import com.example.demo.costant.Costant;
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

public class CalculateMax extends BaseRichBolt {

    private OutputCollector collector;
    private int count;
    private List<Incrocio> listMax;
    List<Incrocio> intersections=null;
    double med;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("max"));
        count=0;
        listMax= new ArrayList<>();
        intersections = new ArrayList<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        //utilizzo il digest nella classe incrocio?


        count++;
        if(input.getSourceStreamId().equals("incrocio")){
            List<Incrocio> intersection;
            intersection= (ArrayList<Incrocio>)input.getValueByField("incrocio");
            intersections.addAll(intersection);
        }else{
            med = (double) input.getValueByField("totalmed");
        }


        if(count > Costant.NUM_FILTER) {
            List<Incrocio> listMax = new ArrayList<>();
            for(int i=0;i!= intersections.size() ;i++){
                if(intersections.get(i).getMedianaVeicoli() > med)
                    listMax.add(intersections.get(i));
            }

            collector.emit(new Values(listMax));
            count=0;
            intersections.clear();
            listMax.clear();
        }
    }
}