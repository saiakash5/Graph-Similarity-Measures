/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ull.Similarity;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Random;

/**
 *
 * @author Akash
 */
public class PriorityHashing implements Serializable {

    HashMap<Integer,Integer> hm = new HashMap<>();
    HashMap<Integer,Double[]> min_max_values= new HashMap<>();
    HashMap<Integer,int[]> min_max_position= new HashMap<>();
    double min_value;
    double max_value;
    int min_position;
    int max_position;
    HashFunction hf;
    PriorityHashing()
    {
        Random r = new Random();
    }

    public  void start(int key,int data)
    {
        insert(key, data);
        insert(data,key);
        //updataMinMax(key,data);
    }
    public void insert(int key,int data)
    {

        if(hm.containsKey(key))
        {
            int i = hm.remove(key);
            i++;
            hm.put(key,i);
        }
        else
        {
            hm.put(key,1);
            Double[] ar = new Double[2];
            int ar1[] = new int[2];
            ar[0]=0.0;
            ar[1]=0.0;
            min_max_values.put(key,ar);
            min_max_position.put(key,ar1);
        }
        if(hm.size()>1)
            updataMinMax(key, data);

    }

    public void updataMinMax(int key,int data)
    {

            double hash = Hash(data);
            int arm[] = min_max_position.get(key);
//            Double[] arv= new Double[2];
//            if(min_max_values.containsKey(key))
            Double[] arv = min_max_values.get(key);

            if(arv[0]==0.0&&arv[1]==0.0)
            {
                arv[0]=hash;
                arm[0]=data;
            }

            else if(hash>arv[0])
            {
                arv[1]=hash;
                arm[1]=data;
            }
            else if (hash<arv[0])
            {
                double temp = arv[0];
                arv[0]=hash;
                arv[1]=temp;
                int t = arm[0];
                arm[0]=data;
                arm[1]=t;
            }
            min_max_position.put(key,arm);

    }

//    public void updateMin(int key,int data,double value)
//    {
//        Double[] arv = min_max_values.get(key);
//        arv[0]=value;
//        int arm[] = min_max_position.get(key);
//        arm[0]=data;
//    }

    public void updateMax(int key,int data)
    {

        int arm[] = min_max_position.get(key);
        int r = hm.remove(key);
        r--;
        hm.put(key,r);
        updataMinMax(key, data);
    }

    public Double getMinHash(int key)
    {
        Double[] ar = min_max_values.get(key);
        return ar[0];
    }

    public int getMinValue(int key)
    {
        int[] ar = min_max_position.get(key);
        return ar[0];
    }

    public Double getMaxHash(int key)
    {
        Double[] ar = min_max_values.get(key);
        return ar[1];
    }
    public int getMaxValue(int key)
    {
        int[] ar = min_max_position.get(key);
        return ar[1];
    }


    public double Hash(int data){
        long MaxValue = 2147483647;
        long MinValue = -2147483648;

        hf = Hashing.murmur3_128();
        int value=hf.hashInt(data).asInt();

        double result=(double)(((value/StreamSize())+1.0)-MinValue)/(double)(MaxValue-MinValue);

        return result;
        //return (double)(thash)/(double)(StreamSize());
    }

    public int StreamSize()
    {
        int sum=0;
        for (int i: hm.keySet())
        {
            sum+=hm.get(i);
        }
        if(sum==0)
            return 1;
        else
            return sum;
    }


    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for(int i:min_max_position.keySet())
        {
            int arm[] = min_max_position.get(i);
            sb.append(i+"--->").append("[").append(arm[0]).append(",").append(arm[1]).append("]");
        }
        return sb.toString();
    }
}
