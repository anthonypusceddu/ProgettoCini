package com.example.demo.bolt;

import com.example.demo.entity.Incrocio;
import com.example.demo.entity.SensoreSemaforo;
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

    private OutputCollector collector;
    private SensoreSemaforo s;
    private ObjectMapper mapper = new ObjectMapper();
    private HashMap<Integer, Incrocio> mappa;
    private Incrocio inc;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","incrocio"));
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
        if ( mappa.containsKey(s.getIncrocio()) ){
            Incrocio c;
            c = mappa.get(s.getIncrocio());
            c.getL().add(s);
            if ( c.getL().size() == 4 ){
                mappa.remove(s.getIncrocio());
                media(c);
                collector.emit(new Values( s.getIncrocio(), inc ) );
            }
            else{
                mappa.put(s.getIncrocio(), inc);
            }
        }
        else {
            lista = new ArrayList<>();
            lista.add(s);
            inc = new Incrocio(lista, s.getIncrocio());
            mappa.put(s.getIncrocio(), inc );
        }
        System.out.println("/n/n");
    }

    private void media(Incrocio c) {
        float somma = 0F;
        int numeroTotaleVeicoli = 0;
        for ( int i = 0 ; i<4 ; i++){
            somma += c.getL().get(i).getVelocità()*c.getL().get(i).getNumeroVeicoli();
            numeroTotaleVeicoli += c.getL().get(i).getNumeroVeicoli();
        }
        c.setNumeroVeicoli(numeroTotaleVeicoli);
        c.setVelocitàMedia(somma/numeroTotaleVeicoli);
    }
}