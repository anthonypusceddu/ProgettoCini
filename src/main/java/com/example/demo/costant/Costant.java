package com.example.demo.costant;

public class Costant {
    public static final int TOP_K = 10;
    public static int N_INTERSECTIONS = 50;
    public static final int SEM_INTERSEC = 4;
    public static final int WINDOW_MIN = 15;
    public static final int WINDOW_HOUR = 1;
    public static final int WINDOW_DAY = 1;
    public static final int NUM_INTERMEDIATERANK = 5;
    public static final int NUM_FILTER = 1;
    public static final int NUM_AVG = 3;
    public static final int SEC_TUPLE = 60;
    public static final int COMPRESSION = 100;
    public static final double QUANTIL = 0.5;
    public static final int NUM_SPOUT_QUERY_1=1;
   // public static final String TOPIC_0 = "classifica";
   // public static final String TOPIC_1 = "mediana";
  //  public static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    public static final String TOPOLOGY_QUERY_2="topologiaMediana";
    public static final String SPOUT_QUERY_2="semaphore";
    public static final String FILTER_QUERY_2="filterBoltMed";
    public static final int NUM_SPOUT_QUERY_2=1;
    public static final String MEDIAN15M_BOLT="Median15MBolt";
    public static final String MEDIAN1H_BOLT="Median1HBolt";
    public static final String MEDIAN24H_BOLT="Median24HBolt";
    public static final String ID="id";
    public static final String GLOBAL15M_MEDIAN="global15MMed";
    public static final String GLOBAL1H_MEDIAN="global1HMed";
    public static final String GLOBAL24H_MEDIAN="global24HMed";
    public static final String SENSOR="sensore";
    public static final String INTERSECTION="incrocio";
    public static final String LIST_INTERSECTION="listaincroci";

    public static final int NUM_GLOBAL_BOLT=1;
    public static final String MEDIAN="median";
    public static final int MESSAGE_TIMEOUT_SEC=1900000;
    public static final String RESULT = "result";
    public static final int NUM_PARALLELISM = 20;
    public static final int NUM_MEDIAN_15M_BOLT = 1;
    public static final int NUM_MEDIAN_1H_BOLT  = 2;
    public static final int NUM_MEDIAN_24H_BOLT = 4;
    public static final String ID15M = "15M";
    public static final String ID1H = "1H";
    public static final String ID24H = "24H";
}
