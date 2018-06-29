package com.example.demo.control.bolt;

import com.example.demo.costant.Costant;
import com.example.demo.entity.Intersection;
import com.example.demo.entity.Sensor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterControlBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<Integer, Intersection> mappa;
    private Intersection inc;


    public FilterControlBolt() {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        mappa = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        List<Sensor> list;
        //controllare integrità tupla e/o semaforo rotto
        Sensor s=(Sensor) input.getValueByField(Costant.SENSOR);
        if ( mappa.containsKey(s.getIntersection()) ){//incrocio esiste in hasmap
            Intersection c;
            c = mappa.get(s.getIntersection());//prendi incrocio dall'hashmap
            c.getL().add(s);//aggiungi il semaforo all'incrocio
            if ( c.getL().size() == Costant.SEM_INTERSEC ){//se l'incrocio è completo
                mappa.remove(s.getIntersection());//rimuovi l'incrocio dall'hashmap
                inc=c;
                collector.emit(new Values( s.getIntersection(), inc ) );//emetti l'incrocio
            }
            else{
                mappa.put(s.getIntersection(), inc);
            }
        }
        else {//l'incrocio non esiste in hashmap
            list = new ArrayList<>();
            list.add(s);
            inc = new Intersection(list, s.getIntersection());//crea incrocio con il semaforo ricevuto
            mappa.put(s.getIntersection(), inc );//metti in hashmap l'incrocio
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.ID,Costant.INTERSECTION));

    }
}
