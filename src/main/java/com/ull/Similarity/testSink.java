package com.ull.Similarity;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Akash
 */
public class testSink {
    public static HashMap<Integer,JaccardSketch> Jdata = new HashMap<Integer,JaccardSketch>();
    public static HashMap<Integer,HyperLogLog> padata = new HashMap<>();
    public static TreeMap<Integer,CommonNeighboursSketch> cdata = new TreeMap<>();
    public static HashMap<Integer,HyperLogLog> aadata = new HashMap<>();
}
