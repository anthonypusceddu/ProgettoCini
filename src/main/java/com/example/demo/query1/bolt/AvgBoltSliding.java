package com.example.demo.query1.bolt;

import com.example.demo.query1.entity.Incrocio;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;

public class AvgBoltSliding extends BaseWindowedBolt {
    private OutputCollector collector;
    private HashMap<Integer, Incrocio> mappa;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("listaincroci"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        mappa = new HashMap<>();
    }

/*
    private List<Incrocio> createList(HashMap<Integer,Incrocio> mappa){
        List<Incrocio> classifica = new ArrayList<>();
        for (Incrocio i : mappa.values()) {
            classifica.add(i);
            System.out.println(i.getVelocitàMedia());
            System.out.println(i.getNumeroVeicoli());
            mappa.remove(i);
        }
        return classifica;
    }

    private Incrocio processAvg(Incrocio oldi,Incrocio i){
        int nTot = oldi.getNumeroVeicoli()+i.getNumeroVeicoli();
        float app=oldi.getVelocitàMedia()*oldi.getNumeroVeicoli()+i.getVelocitàMedia()*i.getNumeroVeicoli();
        i.setVelocitàMedia(app/nTot);
        i.setNumeroVeicoli(nTot);
        return i;
    }*/

    @Override
    public void execute(TupleWindow inputWindow) {

        for(Tuple tuple: inputWindow.get()) {


        }
        // emit the results
        collector.emit(new Values());
    }


}

