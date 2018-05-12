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
        List<Tuple> tupleOldList =inputWindow.getExpired();
        List<Tuple> tupleNewlist=inputWindow.getNew();

        System.out.println("/n/n");
        List<Tuple> tupleList = inputWindow.get();//ottieni la lista di tuple in finestra
        for ( Tuple t : tupleList){
            Incrocio l = (Incrocio) t.getValueByField("incrocio");//ottieni l'incrocio,perchè id incrocio e non id?
            if(mappa.containsKey(l.getId())){//se la mappa contiene l'incrocio
                mappa.put(l.getId(), processAvg(mappa.get(l.getId()),l));//aggiorna la media
            }
            else{//la mappa non contiene l'incrocio,aggiungi nella mappa l'incrocio
                mappa.put(l.getId(),l);
            }
        }
        System.out.println("/n/n");
        //dalla mappa ogni avg bolt avrà vari incroci e li deve raggruppare per ottenere una classifica
        List<Incrocio> classifica = createList(mappa);//crea la classifica
        System.out.println("/n/n");

        System.out.println(classifica);
        collector.emit(new Values(classifica.get(0).getId(),new Rank(classifica)));//emetti la classifica
    }

    private List<Incrocio> createList(HashMap<Integer,Incrocio> mappa){//ritorna una lista di incroci da una hashmap
        List<Incrocio> classifica = new ArrayList<>();
        for (Incrocio i : mappa.values()) {
            classifica.add(i);//aggiungi l'incrocio nell hashmap
            System.out.println(i.getVelocitàMedia());
            System.out.println(i.getNumeroVeicoli());
            mappa.remove(i);//rimuovi l'incrocio dall'hashmap
        }
        return classifica;
    }

    private Incrocio processAvg(Incrocio oldi,Incrocio i){//aggiorna la media pesata tra 2 incroci
        int nTot = oldi.getNumeroVeicoli()+i.getNumeroVeicoli();
        float app=oldi.getVelocitàMedia()*oldi.getNumeroVeicoli()+i.getVelocitàMedia()*i.getNumeroVeicoli();
        i.setVelocitàMedia(app/nTot);
        i.setNumeroVeicoli(nTot);
        return i;
    }
    private Incrocio processAvg_with_sliding(List<Incrocio> oldi,Incrocio i,List<Incrocio> newi){//aggiorna la media pesata tra 2 incroci
        int sumNumOld=0;
        int sumNumNew=0;
        float sumVoldNold=0;
        float sumVnewNnew=0;
        for ( Incrocio inc : oldi){
            sumNumOld+=inc.getNumeroVeicoli();
            sumVoldNold+=inc.getNumeroVeicoli()*inc.getVelocitàMedia();
        }
        for ( Incrocio inc2 : newi){
            sumNumNew+=inc2.getNumeroVeicoli();
            sumVnewNnew+=inc2.getNumeroVeicoli()*inc2.getVelocitàMedia();
        }
        //imposta velocità media
        i.setVelocitàMedia( (i.getNumeroVeicoli()*i.getVelocitàMedia()-sumVoldNold+sumVnewNnew)/(i.getNumeroVeicoli()-sumNumOld+sumNumNew));
        i.setNumeroVeicoli(i.getNumeroVeicoli()-sumNumOld+sumNumNew);//imposta numero veicoli
        return i;
    }
}

