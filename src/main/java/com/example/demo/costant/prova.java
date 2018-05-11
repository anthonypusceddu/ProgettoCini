package com.example.demo.costant;

import com.google.common.collect.Lists;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
public class prova {

    public void tdigest(){
            TDigest td1 = new AVLTreeDigest(100);
            MergingDigest m= new MergingDigest(100);

            td1.add(1);
            td1.add(2);
            td1.add(3);
            td1.add(4);
            td1.add(5);
            System.out.println(td1.quantile(0.5));

            TDigest td2 = new AVLTreeDigest(100);
            td2.add(6);
            td2 .add(7);
            td2.add(8);
            td2.add(9);
            td2.add(10);
            td2.add(11);

        System.out.println(td2.quantile(0.5));
            TDigest td3 = new AVLTreeDigest(100);

            td3.add(td1);
            td3.add(td2);
            System.out.println(td3.quantile(0.5));

            /*for(int i=7;i!=100;i++){
                td1.add(i);
                System.out.println(td1.quantile(0.5));
                System.out.println(td1.size());
            }
            System.out.println(td1.centroidCount());
            System.out.println(td1.getMin());
            td1= null;
            td1 = new AVLTreeDigest(100);
        td1.add(6);
        System.out.println(td1.getMax());*/

    }


    public static void main(String[] args) {

       // p.tdigest();
    }

}
