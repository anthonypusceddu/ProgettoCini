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

public class Median15MBolt extends BaseWindowedBolt {
    //MedianBolt scansiona tutte le tuple incrocio con mediana nella finestra
    // e crea hashmap con id incrocio,incrocio
    //l'incrocio nella hashamp contiene la mediana dei dati relativi a quell'incrocio nel tempo
    //quando i dati in finestra sono finiti invia lista di incroci

    private OutputCollector collector;
    private HashMap<Integer, Incrocio> mappa;


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
            Incrocio l = (Incrocio) t.getValueByField(Costant.INTERSECTION);
            if(mappa.containsKey(l.getId())){
                mappa.put(l.getId(), processMed(mappa.get(l.getId()),l));
            }
            else{
                mappa.put(l.getId(),l);
            }
        }
        List<Incrocio> listamediane = createList(mappa);

        //System.out.println("stampo lista mediane" + listamediane+"size lista "+listamediane.size());
        collector.emit(new Values(listamediane));
        //System.out.println("ho inviato dal Median15MBolt ");
    }

    private List<Incrocio> createList(HashMap<Integer,Incrocio> mappa){
        List<Incrocio> med = new ArrayList<>();
        for (Incrocio i : mappa.values()) {
            i.setMedianaVeicoli(i.getTd1().quantile(Costant.QUANTIL));
            med.add(i);
            mappa.remove(i);
        }
        return med;
    }

    private Incrocio processMed(Incrocio oldi,Incrocio newi){
        oldi.getTd1().add(newi.getTd1());
        return oldi;
    }
}


