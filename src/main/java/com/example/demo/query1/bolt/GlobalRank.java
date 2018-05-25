package com.example.demo.query1.bolt;

import com.example.demo.query1.ComparatoreIncrocio;
import com.example.demo.query1.entity.Incrocio;
import com.example.demo.costant.Costant;
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
    private String AvgType;
    private int repNum;


    public GlobalRank(String avgBolt, int rep) {
        this.AvgType = avgBolt;
        this.repNum = rep;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.ID,Costant.RANK_TOPK));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        globalRanking = new ArrayList<>();
        countIntermediateRank = 0;
    }

    @Override
    public void execute(Tuple tuple) {

        List<Incrocio> list = ( List<Incrocio> ) tuple.getValueByField(Costant.PARTIAL_RANK);
        countIntermediateRank++;
        if(globalRanking.isEmpty()) {
            globalRanking = list;
        }else{
            sortOrderedRank(list);
        }
        if(countIntermediateRank >= repNum) {
            collector.emit(new Values(this.AvgType,globalRanking));
            System.out.println(this.AvgType + "    " + globalRanking);
            globalRanking = null;
            globalRanking = new ArrayList<>();
            countIntermediateRank = 0;
        }

    }

    private void sortOrderedRank( List<Incrocio> list) {
        if(globalRanking.size() < Costant.TOP_K) {
            //sorting e sublist10
            globalRanking=unionAndSort(globalRanking,list);
        }else if(list.get(0).getVelocitaMedia() <= globalRanking.get(Costant.TOP_K-1).getVelocitaMedia()){
                globalRanking=unionAndSort(globalRanking,list);
        }
    }

    private List<Incrocio> unionAndSort(List<Incrocio> list1, List<Incrocio> list2){
        List<Incrocio> l = ListUtils.union(list1,list2);
        Collections.sort(l,new ComparatoreIncrocio());
        if(l.size()> Costant.TOP_K)
            l = l.subList(0,Costant.TOP_K);
        return l;
    }

}
