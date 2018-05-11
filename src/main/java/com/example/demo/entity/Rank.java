package com.example.demo.entity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Rank implements Comparator<Incrocio> {
    private List<Incrocio> listIntersection;

    public Rank(){
        listIntersection= new ArrayList<Incrocio>();
    }

    @Override
    public int compare(Incrocio incrocio, Incrocio t1) {
        if ( incrocio.getVelocitàMedia() > t1.getVelocitàMedia() )
            return 1;
        else if ( incrocio.getVelocitàMedia() < t1.getVelocitàMedia() )
            return -1;
        else
            return 0;
    }
}
