/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ull.Similarity;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.TreeMap;
import java.util.TreeSet;


/**
 *
 * @author Akash
 */
public class CommonNeighboursSketch {
    private int size =1000;
    private int hsize=10;
    HashFuctionList hashlist = null;
    private Double LowerEta = 1.0;
    private Double HigherEta = 1.0;

    public CommonNeighboursSketch(int size){
              this.size=size;
              
            
              
          }

    CommonNeighboursSketch()
    {
        edge_reservoir = new TreeSet<>();
        treeMap = new TreeMap<>();
    }
    
    public CommonNeighboursSketch(int size,int hsize){
        this.size= size;
        this.hsize=10;
        
    }
    TreeSet<Integer> edge_reservoir;
    TreeMap<Integer,Double> treeMap ;
    long arr[] =  new long[2];

    public void setData(int data)
    {
        edge_reservoir=new TreeSet<>();
        edge_reservoir.add(data);
    }

    public void initialize(int data)
    {
        edge_reservoir=new TreeSet<>();
        edge_reservoir.add(data);
        double hash;
        treeMap = new TreeMap<>();
        treeMap.put(data, hash(data));

    }
    
    public void addData(int index,int data,PriorityHashing ph){
        

           if(!edge_reservoir.contains(data))
           {
               if(edge_reservoir.size()<size-1)
               {
                   edge_reservoir.add(data);
                                     
               }
               else
               {
                   if(ph.Hash(data)<ph.getMinHash(index))
                   {
                       edge_reservoir.remove(ph.getMaxValue(index));
                       edge_reservoir.add(data);
                       ph.updateMax(index,data);
                       ph.updataMinMax(index,data);
                   }
               }
           }
    }




    public void Union(CommonNeighboursSketch other){

        //long start = System.currentTimeMillis();

        TreeSet<Integer> Oedge_reservoir = other.edge_reservoir;
        TreeMap OtreeMap = other.treeMap;
        for (Integer a:Oedge_reservoir) {
            SampleAddData(a);
        }

       // System.out.println("Union time:"+ (start-System.currentTimeMillis()));
    }
    public void add(int data,int reservoir_size,TreeMap<Integer,HyperLogLog> degree,HyperLogLog total)
    {
        if(edge_reservoir.size()<reservoir_size) {
            edge_reservoir.add(data);


        }
        else {
            update(data, degree, total);

        }
    }

    public void update(int data, TreeMap<Integer,HyperLogLog> degree,HyperLogLog total)
    {
        if(getPriority(data,degree,total)<=getMinHash(degree,total))
        {
            edge_reservoir.remove(getMax(degree,total));
            edge_reservoir.add(data);
        }
    }

    public int getMin(TreeMap<Integer,HyperLogLog> degree,HyperLogLog total)
    {
        int min_value=-1;
        long min=Long.MAX_VALUE;
        long total_edges = total.cardinality();
        for(int i: edge_reservoir)
        {
            HyperLogLog hp = degree.get(i);
            long edge_degree=hp.cardinality();
            long hash = 1-(edge_degree/total_edges);
            if(hash<min)
            {
                min=hash;
                min_value=i;
            }
        }
        arr[0] = min_value;
        return min_value;
    }

    public int getMax(TreeMap<Integer,HyperLogLog> degree,HyperLogLog total)
    {
        int max_value=-1;
        long max = Long.MIN_VALUE;
        long total_edges = total.cardinality();
        for(int i: edge_reservoir)
        {
            HyperLogLog hp = degree.get(i);
            long edge_degree=hp.cardinality();
            long hash = 1-(edge_degree/total_edges);
            if(hash>max)
            {
                max=hash;
                max_value=i;
            }
        }
        arr[1]=max_value;
        return max_value;
    }

    public double getMinHash(TreeMap<Integer,HyperLogLog> degree,HyperLogLog total)
    {
//        int min_value=-1;
//        long min=Long.MAX_VALUE;
//        long total_edges = total.cardinality();
//        for(int i: edge_reservoir)
//        {
//            HyperLogLog hp = degree.get(i);
//            long edge_degree=hp.cardinality();
//            long hash = 1-(edge_degree/total_edges);
//            if(hash<min)
//            {
//                min=hash;
//                min_value=i;
//            }
//        }
        return LowerEta;
    }

    public double getMaxHash(TreeMap<Integer,HyperLogLog> degree,HyperLogLog total)
    {
//        int max_value=-1;
//        long max = Long.MIN_VALUE;
//        long total_edges = total.cardinality();
//        for(int i: edge_reservoir)
//        {
//            HyperLogLog hp = degree.get(i);
//            long edge_degree=hp.cardinality();
//            long hash = 1-(edge_degree/total_edges);
//            if(hash>max)
//            {
//                max=hash;
//                max_value=i;
//            }
//        }
        return HigherEta;
    }

    public long getPriority(int data,TreeMap<Integer,HyperLogLog> degree,HyperLogLog total)
    {
        long total_edges=total.cardinality();
        HyperLogLog hp = degree.get(data);
        return 1-(hp.cardinality()-total_edges);
    }



    public TreeSet<Integer> getData()
    {
        return edge_reservoir ;
    }

//    public void processData(int index,int data)
//    {
//               ArrayList<Integer> temp = edge_reservoir.get(index);
//               
//               if(temp.size()<=size&&!temp.contains(data))
//               {
//                   temp.add(data);
//                   edge_reservoir.replace(index, temp);
//               }
//               else
//               {
//                   if(HashFunction(data,edge_degree.get(data))<min_max.get(index).get(0))
//                   {
//                       double arr[] = minmaxHashValue(temp);
//                       ArrayList<Double> res = new ArrayList<>();
//                       int max = (int)arr[1];
//                       temp.remove(new Integer(max));
//                       temp.add(data);
//                       double MinMax[] = minmaxHashValue(temp);
//                       for(int i=0;i<MinMax.length;i++)
//                       {
//                           res.add(MinMax[i]);
//                       }
//                       min_max.replace(index, res);
//                   }
//               }
//               
//               
//           
//    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder().append('[');
        sb.append(edge_reservoir);

        sb.append("min->"+getMinDataTreeMap()+",");
        sb.append("max->"+getMaxDataTreeMap());

//        for(int i:edge_reservoir)
//        {
//
//        }
        sb.append(']');

        return sb.toString();
    }

    //Sample methods//



    //Sample1
    public void SampleAddData(int data){


        if(!edge_reservoir.contains(data))
        {
            if(edge_reservoir.size()< size)
            {
                edge_reservoir.add(data);
                treeMap.put(data,hash(data));

            }
            else
            {

                double data_hash = hash(data);

                if(data_hash <= getMinHashDataTreeMap()){
                     int maxIndex = getMaxDataTreeMap();
                     edge_reservoir.remove(maxIndex);
                     treeMap.remove(maxIndex);
                     edge_reservoir.add(data);
                     treeMap.put(data,hash(data));
                     HigherEta = hash(maxIndex);
                    maxIndex = getMaxDataTreeMap();
                    LowerEta = hash(maxIndex);
                }
            }
        }
    }

    public Double hash(int data){
        long MaxValue = 2147483647;
        long MinValue = -2147483648;
        HashFunction hf = Hashing.murmur3_128(data*1234);
        Integer value = hf.hashInt(data).asInt();
        return (double)(value-MinValue)/(double)(MaxValue-MinValue);
    }



    public int getMinDataTreeMap()
    {
        int min_value = -1;
        double value = Double.MAX_VALUE;
        for(int i: treeMap.keySet())
        {
            if(treeMap.get(i)<value)
            {
                value=treeMap.get(i);
                min_value=i;
            }
        }
        return min_value;
    }

    public Double getMinHashDataTreeMap()
    {
        int min_value = -1;
        double value = Double.MAX_VALUE;
        for(int i: treeMap.keySet())
        {
            if(treeMap.get(i)<value)
            {
                value=treeMap.get(i);
                min_value=i;
            }
        }
        return value;
    }


    public int getMaxDataTreeMap()
    {
        int index = -1;
        double value = Double.MIN_VALUE;

        for(int i: treeMap.keySet())
        {
            if(treeMap.get(i)>value)
            {
                value = treeMap.get(i);
                index=i;
            }
        }

        return index;
    }

    public double getMaxHashDataTreeMap()
    {
        int max_value = -1;
        double value = Double.MIN_VALUE;
        for(int i: treeMap.keySet())
        {
            if(treeMap.get(i)>value)
            {
                value=treeMap.get(i);
                max_value=i;
            }
        }
        return value;
    }


}
