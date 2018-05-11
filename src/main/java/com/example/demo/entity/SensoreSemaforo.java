package com.example.demo.entity;


public class SensoreSemaforo {

    private int incrocio;
    private int semaforo;
    private float velocità;
    private int numeroVeicoli;

    public SensoreSemaforo(int i, int s,float vel, int nv){
        this.incrocio = i;
        this.semaforo = s;
        this.velocità = vel;
        this.numeroVeicoli = nv;
    }

    public SensoreSemaforo(){
    }


    public int getIncrocio() {
        return incrocio;
    }

    public void setIncrocio(int incrocio) {
        this.incrocio = incrocio;
    }

    public int getSemaforo() {
        return semaforo;
    }

    public void setSemaforo(int semaforo) {
        this.semaforo = semaforo;
    }

    public float getVelocità() {
        return velocità;
    }

    public void setVelocità(float velocità) {
        this.velocità = velocità;
    }

    public int getNumeroVeicoli() {
        return numeroVeicoli;
    }

    public void setNumeroVeicoli(int numeroVeicoli) {
        this.numeroVeicoli = numeroVeicoli;
    }
}
