/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ull.Similarity;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *
 * @author Akash
 */
public class test {
    
    public static void main(String[] args) throws InterruptedException{
        
        HashFuctionList hashlist = new HashFuctionList(10);

        for(int i=0; i<100;i++){
            System.out.println(hashlist.hash(3, 2));
         
        }
        System.out.println(Integer.MIN_VALUE);
        System.out.println(Integer.MAX_VALUE);
    }
}
