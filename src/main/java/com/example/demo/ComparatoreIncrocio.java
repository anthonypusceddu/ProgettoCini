package com.example.demo;

import com.example.demo.entity.Incrocio;

import java.util.Comparator;

public class ComparatoreIncrocio implements Comparator<Incrocio> {
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