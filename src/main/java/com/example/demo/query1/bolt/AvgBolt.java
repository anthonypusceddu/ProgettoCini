package com.example.demo.query1.bolt;

import com.example.demo.query1.entity.Incrocio;
import com.example.demo.query1.entity.Rank;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.*;

public class AvgBolt extends BaseWindowedBolt {

    private OutputCollector collector;
    private HashMap<Integer, Incrocio> mappa;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","listaincroci"));//?perchè serve anche incrocio se l'avg bolt ha fieldgrouping by id
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        mappa = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        System.out.println("/n/n");
        List<Tuple> tupleList = inputWindow.get();
        for ( Tuple t : tupleList){
            Incrocio l = (Incrocio) t.getValueByField("incrocio");
            if(mappa.containsKey(l.getId())){
                mappa.put(l.getId(), processAvg(mappa.get(l.getId()),l));
            }
            else{
                mappa.put(l.getId(),l);
            }
        }
        System.out.println("/n/n");

        List<Incrocio> classifica = createList(mappa);
        System.out.println("/n/n");

        System.out.println(classifica);
        collector.emit(new Values(classifica.get(0).getId(),new Rank(classifica)));
    }

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
    }
}

