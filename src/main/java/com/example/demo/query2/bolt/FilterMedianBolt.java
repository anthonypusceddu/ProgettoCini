package com.example.demo.query2.bolt;

import com.example.demo.costant.Costant;
import com.example.demo.entity.Intersection;
import com.example.demo.entity.Sensor;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
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

public class FilterMedianBolt extends BaseRichBolt {
    //bolt filtro riceve tupla di un sensore e invia una tupla incrocio quando ha ricevuto i 4 semafori
    //dello stesso incrocio
    private OutputCollector collector;
    //private Sensor s;
    //private ObjectMapper mapper = new ObjectMapper();
    private HashMap<Integer, Intersection> mappa;
    private Intersection inc;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.ID,Costant.INTERSECTION));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        mappa = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        List<Sensor> lista = null;
        //controllare integrità tupla e/o semaforo rotto
        //JsonNode jsonNode = (JsonNode) input.getValueByField("sensore");
        Sensor s=(Sensor) input.getValueByField(Costant.SENSOR);

        if ( mappa.containsKey(s.getIntersection()) ){
            Intersection c;
            c = mappa.get(s.getIntersection());
            c.getL().add(s);
            if ( c.getL().size() == Costant.SEM_INTERSEC ){
                mappa.remove(s.getIntersection());
                mediana(c);
                collector.emit(new Values(c.getId(),c ) );
                //System.out.println("FilterMedianBoltHaEMesso");
            }
            else{
                //sarebbe più sicuro fare remove e riaggiungere
                mappa.remove(c.getId());
                mappa.put(s.getIntersection(),c);
            }
        }
        else {
            lista = new ArrayList<>();
            lista.add(s);
            inc = new Intersection(lista, s.getIntersection());
            mappa.put(s.getIntersection(), inc );
        }
    }

    private void mediana(Intersection c){
        //calcola mediana dell'incrocio
        TDigest td1 = new AVLTreeDigest(Costant.COMPRESSION);
        for(int i=0;i!=Costant.SEM_INTERSEC;i++){//cambiare il 4 con Constant. ecc
            td1.add(c.getL().get(i).getNumVehicles());
        }
        c.setMedianaVeicoli(td1.quantile(Costant.QUANTIL));
        c.setTd1(td1);
    }

}