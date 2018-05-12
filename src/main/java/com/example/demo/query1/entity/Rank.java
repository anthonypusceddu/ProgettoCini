package com.example.demo.query1.entity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Rank implements Comparator<Incrocio> {
    private List<Incrocio> listIntersection;

    public Rank(){
        listIntersection= new ArrayList<Incrocio>();
    }
    public Rank(List<Incrocio> listIntersection){
        this.listIntersection=listIntersection;
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

    public List<Incrocio> getListIntersection() {
        return listIntersection;
    }

    public void setListIntersection(List<Incrocio> listIntersection) {
        this.listIntersection = listIntersection;
    }
}
