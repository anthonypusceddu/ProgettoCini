package com.example.demo.query1.entity;


public class SensoreSemaforo {

    private int incrocio;
    private int semaforo;
    private double velocita;
    private int numeroVeicoli;

    public SensoreSemaforo(int i, int s,double vel, int nv){
        this.incrocio = i;
        this.semaforo = s;
        this.velocita = vel;
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

    public double getVelocita() {
        return velocita;
    }

    public void setVelocita(double velocita) {
        this.velocita = velocita;
    }

    public int getNumeroVeicoli() {
        return numeroVeicoli;
    }

    public void setNumeroVeicoli(int numeroVeicoli) {
        this.numeroVeicoli = numeroVeicoli;
    }


    @Override
    public String toString(){
        return "velocità "+this.velocita+" num="+this.numeroVeicoli;
    }

}
