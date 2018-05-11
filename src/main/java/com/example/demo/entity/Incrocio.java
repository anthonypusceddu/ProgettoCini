package com.example.demo.entity;

import com.example.demo.costant.Costant;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

import java.util.List;

public class Incrocio {
    private List<SensoreSemaforo> l;
    private int id;
    private float VelocitàMedia;
    private double medianaVeicoli;
    private int numeroVeicoli;
    private TDigest td1 ;

    public Incrocio(List<SensoreSemaforo> l, int id) {
        this.l = l;
        this.id = id;
        this.td1= new AVLTreeDigest(Costant.COMPRESSION);
    }

    public List<SensoreSemaforo> getL() {
        return l;
    }

    public void setL(List<SensoreSemaforo> l) {
        this.l = l;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public float getVelocitàMedia() {
        return VelocitàMedia;
    }

    public void setVelocitàMedia(float velocitàMedia) {
        VelocitàMedia = velocitàMedia;
    }

    public int getNumeroVeicoli() {
        return numeroVeicoli;
    }

    public void setNumeroVeicoli(int numeroVeicoli) {
        this.numeroVeicoli = numeroVeicoli;
    }

    public double getMedianaVeicoli() {
        return medianaVeicoli;
    }

    public void setMedianaVeicoli(double medianaVeicoli) {
        this.medianaVeicoli = medianaVeicoli;
    }

    public TDigest getTd1() {
        return td1;
    }

    public void setTd1(TDigest td1) {
        this.td1 = td1;
    }
}
