package com.example.demo.control.bolt;

import com.example.demo.costant.Costant;
import com.example.demo.entity.Intersection;
import com.example.demo.entity.Sensor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SumBolt extends BaseWindowedBolt{
    private OutputCollector collector;
    private HashMap<Integer, Intersection> map;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.MAP_INTERSECTION));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        map = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tupleList = inputWindow.get();
        for ( Tuple t : tupleList){
            Intersection l = (Intersection) t.getValueByField(Costant.INTERSECTION);
            if(map.containsKey(l.getId())){
                map.put(l.getId(), sumVehicles(map.get(l.getId()),l));
            }
            else{
                map.put(l.getId(),l);
            }
        }
        collector.emit(new Values(map));
    }


    private Intersection sumVehicles(Intersection oldi, Intersection newi){
        List<Sensor> sOld = oldi.getL();
        List<Sensor> sNew = newi.getL();

        for (int i = 0; i < Costant.SEM_INTERSEC; i++) {
            sOld.get(i).setNumVehicles(sNew.get(i).getNumVehicles() + sOld.get(i).getNumVehicles());
        }
        return oldi;
    }
}
