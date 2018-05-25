package com.example.demo.query1;

import com.example.demo.query1.entity.Incrocio;

import java.util.Comparator;

public class ComparatoreIncrocio implements Comparator<Incrocio> {
    @Override
    public int compare(Incrocio incrocio, Incrocio t1) {
        if ( incrocio.getVelocitaMedia() > t1.getVelocitaMedia() )
            return -1;
        else if ( incrocio.getVelocitaMedia() < t1.getVelocitaMedia() )
            return 1;
        else
            return 0;
    }


}