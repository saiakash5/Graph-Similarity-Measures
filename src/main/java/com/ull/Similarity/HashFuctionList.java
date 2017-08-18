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

/**
 *
 * @author Akash
 */
public class HashFuctionList implements Serializable{
    int size = 10;
    long MaxValue = 2147483647;
    long MinValue = -2147483648;
    HashMap<Integer, HashFunction> funcs = null;
    
    public HashFuctionList(int size){
        build(size);
    }
    public void build(int size){
        
        if(funcs == null){
            funcs = new HashMap<>(size);
            for(int i=0; i<size; i++){

                HashFunction hf = Hashing.murmur3_128(i*10203+1234);
                funcs.put(i, hf);
            }
        }
    }
    
    public Double hash(Integer word, Integer index){
        Integer result = funcs.get(index).hashInt(word).asInt();
        
        return (double)(result-MinValue)/(double)(MaxValue-MinValue);
    }
    
}
