package com.example.demo.costant;

public class Costant {
    public static final int TOP_K = 10;
    public static final int N_INTERSECTIONS = 50;
    public static final int SEM_INTERSEC = 4;
    public static final int WINDOW_MIN = 25;
    public static final int WINDOW_HOUR = 1;
    public static final int WINDOW_DAY = 1;
    public static final int NUM_INTERMEDIATERANK = 5;
    public static final int NUM_FILTER = 1;
    public static final int NUM_AVG = 3;
    public static final int SEC_TUPLE = 60;
    public static final int COMPRESSION = 100;
    public static final double QUANTIL = 0.5;
    public static final int NUM_SPOUT_QUERY_1=1;
    public static final String TOPIC_0 = "classifica";
    public static final String TOPIC_1 = "mediana";
    public static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    public static final String TOPOLOGY_QUERY_2="topologiaMediana";
    public static final String SPOUT_QUERY_2="semaphore";
    public static final String FILTER_QUERY_2="filterBoltMed";
    public static final int NUM_SPOUT_QUERY_2=1;
    public static final String MEDIAN_BOLT="MedianBolt";
    public static final String ID="id";
    public static final String GLOBAL_MEDIAN="globalMed";
    public static final String COMPARE_BOLT="compareBolt";
    public static final String SENSOR="sensore";
    public static final String INTERSECTION="incrocio";
    public static final String LIST_INTERSECTION="listaincroci";
    public static final int NUM_MEDIAN_BOLT=3;
    public static final int NUM_GLOBAL_BOLT=1;
    public static final String MEDIAN="median";
    public static final int MESSAGE_TIMEOUT_SEC=1900000;
    public static final int NUM_COMPARE_MEDIAN_BOLT=1;
    public static final String RESULT = "result";
}
