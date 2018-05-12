package com.example.demo.query1.bolt;

import com.example.demo.costant.Costant;
import com.example.demo.query1.entity.Incrocio;
import com.example.demo.query1.entity.SensoreSemaforo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

public class FilterBolt extends BaseRichBolt {
//il filter bolt riceve semafori e invia incroci quando essi sono completi
    private OutputCollector collector;
    private SensoreSemaforo s;
    private ObjectMapper mapper = new ObjectMapper();
    private HashMap<Integer, Incrocio> mappa;
    private Incrocio inc;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","incrocio"));//?perchè serve anche incrocio se l'avg bolt ha fieldgrouping by id
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        mappa = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        List<SensoreSemaforo> lista = null;
        //controllare integrità tupla e/o semaforo rotto
        System.out.println("/n/n");
        System.out.println(input.getValueByField("value"));
        JsonNode jsonNode = (JsonNode) input.getValueByField("value");
        try {
            this.s = mapper.treeToValue(jsonNode,SensoreSemaforo.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        if ( mappa.containsKey(s.getIncrocio()) ){//incrocio esiste in hasmap
            Incrocio c;
            c = mappa.get(s.getIncrocio());//prendi incrocio dall'hashmap
            c.getL().add(s);//aggiungi il semaforo all'incrocio
            if ( c.getL().size() == Costant.SEM_INTERSEC ){//se l'incrocio è completo
                mappa.remove(s.getIncrocio());//rimuovi l'incrocio dall'hashmap
                media(c);//calcola la media
                inc=c;
                collector.emit(new Values( s.getIncrocio(), inc ) );//emetti l'incrocio
            }
            else{//?
                mappa.put(s.getIncrocio(), inc);
            }
        }
        else {//l'incrocio non esiste in hashmap
            lista = new ArrayList<>();
            lista.add(s);
            inc = new Incrocio(lista, s.getIncrocio());//crea incrocio con il semaforo ricevuto
            mappa.put(s.getIncrocio(), inc );//metti in hashmap l'incrocio
        }
        System.out.println("/n/n");
    }

    private void media(Incrocio c) {
        float somma = 0F;
        int numeroTotaleVeicoli = 0;
        for ( int i = 0 ; i<Costant.SEM_INTERSEC ; i++){
            somma += c.getL().get(i).getVelocità()*c.getL().get(i).getNumeroVeicoli();
            numeroTotaleVeicoli += c.getL().get(i).getNumeroVeicoli();
        }
        c.setNumeroVeicoli(numeroTotaleVeicoli);
        c.setVelocitàMedia(somma/numeroTotaleVeicoli);
    }
}