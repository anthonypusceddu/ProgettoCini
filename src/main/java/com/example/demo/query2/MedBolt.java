package com.example.demo.query2;

import com.example.demo.costant.Costant;
import com.example.demo.query1.entity.Incrocio;
import com.example.demo.query1.entity.Rank;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.*;

public class MedBolt extends BaseWindowedBolt {

    private OutputCollector collector;
    private HashMap<Integer, Incrocio> mappa;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","listaincroci"));
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
                mappa.put(l.getId(), processMed(l));
            }
            else{
                mappa.put(l.getId(),l);
                processMed(l);
            }
        }
        System.out.println("/n/n");

        List<Incrocio> classifica = createList(mappa);
        System.out.println("/n/n");

        System.out.println(classifica);
        collector.emit(new Values(classifica.get(0).getId(),new Rank(classifica)));
    }

    private List<Incrocio> createList(HashMap<Integer,Incrocio> mappa){
        List<Incrocio> med = new ArrayList<>();
        for (Incrocio i : mappa.values()) {
            med.add(i);
            mappa.remove(i);
        }
        return med;
    }

    private Incrocio processMed(Incrocio i){
        TDigest td1 = new AVLTreeDigest(Costant.COMPRESSION);
        td1.add(i.getMedianaVeicoli());
        i.setMedianaVeicoli(td1.quantile(Costant.QUANTIL));
        i.setTd1(td1);
        return i;
    }
}


