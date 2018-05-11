package com.example.demo.entity;

import java.util.List;

public class Incrocio {
    private List<SensoreSemaforo> l;
    private int id;
    private float VelocitàMedia;
    private int numeroVeicoli;

    public Incrocio(List<SensoreSemaforo> l, int id) {
        this.l = l;
        this.id = id;
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
}
