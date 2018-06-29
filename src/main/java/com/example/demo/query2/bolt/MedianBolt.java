package com.example.demo.query2.bolt;

import com.example.demo.costant.Costant;
import com.example.demo.entity.Intersection;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MedianBolt extends BaseWindowedBolt {

    private OutputCollector collector;
    private HashMap<Integer, Intersection> mappa;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.LIST_INTERSECTION));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        mappa = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tupleList = inputWindow.get();
        for ( Tuple t : tupleList){
            Intersection l = (Intersection) t.getValueByField(Costant.INTERSECTION);
            if(mappa.containsKey(l.getId())){
                mappa.put(l.getId(), processMed(mappa.get(l.getId()),l));
            }
            else{
                mappa.put(l.getId(),l);
            }
        }
        List<Intersection> listamediane = createList(mappa);
        collector.emit(new Values(listamediane));


    }

    private List<Intersection> createList(HashMap<Integer,Intersection> mappa){
        List<Intersection> med = new ArrayList<>();
        for (Intersection i : mappa.values()) {
            i.setMedianaVeicoli(i.getTd1().quantile(Costant.QUANTIL));
            med.add(i);
            mappa.remove(i);
        }
        return med;
    }

    private Intersection processMed(Intersection oldi, Intersection newi){
        oldi.getTd1().add(newi.getTd1());
        return oldi;
    }
}
