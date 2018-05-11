package com.example.demo.bolt;

import com.example.demo.ComparatoreIncrocio;
import com.example.demo.entity.Incrocio;
import com.example.demo.costant.Costant;
import com.example.demo.entity.Rank;
import org.apache.storm.shade.org.apache.commons.collections.ListUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class GlobalRank extends BaseRichBolt {
    private OutputCollector collector;
    private int countIntermediateRank;
    private List<Incrocio> globalRanking;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        globalRanking = new ArrayList<>();
        countIntermediateRank = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        countIntermediateRank++;
        if(countIntermediateRank > Costant.NUM_INTERMEDIATERANK) {
            globalRanking.clear();
            countIntermediateRank = 0;
        }
        Rank rank=(Rank)tuple.getValueByField("classificaparziale");
        List<Incrocio> list=rank.getListIntersection();
        if(globalRanking.isEmpty()) {
            globalRanking = list;
            collector.emit(new Values(globalRanking));
        }
        else{
            sortOrderedRank(globalRanking,list);
            collector.emit(new Values(globalRanking));
        }
    }

    private void sortOrderedRank(List<Incrocio> globalRanking, List<Incrocio> list) {
        if(globalRanking.size() < Costant.TOP_K) {
            //sorting e sublist10
            globalRanking=unionAndSort(globalRanking,list);
        }else if(list.get(0).getVelocitàMedia() <= globalRanking.get(Costant.TOP_K-1).getVelocitàMedia()){
                globalRanking=unionAndSort(globalRanking,list);
        }
        collector.emit(new Values(globalRanking));
    }

    private List<Incrocio> unionAndSort(List<Incrocio> list1, List<Incrocio> list2){
        List<Incrocio> l = ListUtils.union(list1,list2);
        Collections.sort(l,new ComparatoreIncrocio());
        if(l.size()> Costant.TOP_K)
            l.subList(0,Costant.TOP_K-1);
        return l;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("classificaparziale"));
    }
}
