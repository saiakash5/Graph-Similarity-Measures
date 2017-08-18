/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ull.Similarity;

import java.util.TreeMap;

/**
 *
 * @author Akash
 */
public class JaccardSketch {
    
     private int size = 0;
          public JaccardSketch(){
              this(10);
          }
          
          public JaccardSketch(int size){
              this.size = size;
          }
          
          public TreeMap<Integer,Integer> Hash_position = new TreeMap<>();
          private TreeMap<Integer,Double> Hash_values = new TreeMap<>();
          
         public void addData(Integer index,Integer position,Double value){
             //Check
             if(index < 0 || index > size-1){
                 System.err.println("Error in index value in function addData");
                 return;
             }
             if(!Hash_position.containsKey(index)){
                 Hash_position.put(index, position);
                 Hash_values.put(index, value);
             }else{
                 Hash_position.replace(index, position);
                 Hash_values.replace(index, value);
             }
             
         }
         public Integer getPosition(Integer index){
             
             return Hash_position.get(index);
         }
         public Double getValue(Integer index){
             return Hash_values.get(index);
         }


         public void Union(JaccardSketch otherSketch){


             for (int i = 0; i < size; i++) { //for hash function k=i

                 Double uValue = getValue(i);  // HashValue of U
                 Double vValue = otherSketch.getValue(i); // HashValue of V

                 if (vValue < uValue) {

                     addData(i,otherSketch.getPosition(i),vValue);
                     //t.f1.addData(i, t1.f1.getPosition(i), t1_value);
                 }
             }
         }
         @Override
        public String toString(){
            StringBuilder sb = new StringBuilder().append('[');
            for(int i=0; i< size; i++){
                sb.append(Hash_position.get(i)+"("+Hash_values.get(i)+")");
                sb.append(",");
            }
            sb.append(']');
            sb.append("\n");
            return sb.toString();
        }
         
}
