package com.example.demo.control.bolt;

import com.example.demo.costant.Costant;
import com.example.demo.entity.Intersection;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.List;

public class SumBolt extends BaseWindowedBolt{
    @Override
    public void execute(TupleWindow inputWindow) {
        ArrayList<Tuple> tupleList = (ArrayList<Tuple>) inputWindow.get();


    }
}
