package com.example.demo.query2;

import com.example.demo.query1.entity.SensoreSemaforo;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SpoutSem extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random rand;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector=spoutOutputCollector;
        rand=new Random();
    }

    @Override
    public void nextTuple() {
        float max = 100F;
        float min = 0F;
        SensoreSemaforo s;
        Utils.sleep(1000);
        for (int i = 0; i < 10; i++) {
            for ( int j = 0 ; j < 4 ; j++){
                s=new SensoreSemaforo(i,j,min + rand.nextFloat() * (max - min), ThreadLocalRandom.current().nextInt(0, 100 + 1)) ;
                collector.emit(new Values(s));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sensore"));
    }
}
