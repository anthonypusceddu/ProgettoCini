package com.example.demo.query1.bolt;

import com.example.demo.costant.Costant;
import com.example.demo.query1.entity.Incrocio;
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
        declarer.declare(new Fields(Costant.LIST_INTERSECTION));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        mappa = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        List<Tuple> tupleList = inputWindow.get();//ottieni la lista di tuple in finestra
        for ( Tuple t : tupleList){
            Incrocio l = (Incrocio) t.getValueByField(Costant.INTERSECTION);
            if(mappa.containsKey(l.getId())){//se la mappa contiene l'incrocio
                mappa.put(l.getId(), processAvg(mappa.get(l.getId()),l));//aggiorna la media
            }
            else{//la mappa non contiene l'incrocio,aggiungi nella mappa l'incrocio
                mappa.put(l.getId(),l);
            }
        }
        //dalla mappa ogni avg bolt avrà vari incroci e li deve raggruppare per ottenere una classifica
        List<Incrocio> classifica = createList(mappa);//crea la classifica
        collector.emit(new Values(classifica));//emetti la classifica
    }

    private List<Incrocio> createList(HashMap<Integer,Incrocio> mappa){//ritorna una lista di incroci da una hashmap
        List<Incrocio> classifica = new ArrayList<>();
        for (Incrocio i : mappa.values()) {
            classifica.add(i);//aggiungi l'incrocio nell hashmap
            mappa.remove(i);//rimuovi l'incrocio dall'hashmap
        }
        return classifica;
    }

    private Incrocio processAvg(Incrocio oldi,Incrocio i){//aggiorna la media pesata tra 2 incroci
        int nTot = oldi.getNumeroVeicoli()+i.getNumeroVeicoli();
        double app=oldi.getVelocitaMedia()*oldi.getNumeroVeicoli()+i.getVelocitaMedia()*i.getNumeroVeicoli();
        i.setVelocitaMedia(app/nTot);
        i.setNumeroVeicoli(nTot);
        return i;
    }

}

