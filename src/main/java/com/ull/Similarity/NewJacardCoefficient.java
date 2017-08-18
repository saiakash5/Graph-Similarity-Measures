/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ull.Similarity;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on http://www.kdd.org/kdd2016/papers/files/rfp0465-de-stefaniA.pdf.
 */
public class NewJacardCoefficient {

    int size = 10;

    public NewJacardCoefficient(int size) {
        this.size = size;
    }

    public DataStream<Tuple2<Integer, JaccardSketch>> process(KeyedStream<Edge<Integer, Integer>, Integer> stream) {
        return stream.flatMap(new JCFlatMapper(size))
                .keyBy(new KeySelector<Tuple2<Integer,JaccardSketch>, Integer>() {


                    @Override
                    public Integer getKey(Tuple2<Integer, JaccardSketch> integerJaccardSketchTuple2) throws Exception {
                        return integerJaccardSketchTuple2.f0;
                    }
                }).reduce(new JCReducer(size));
    }

}

class JCFlatMapper implements FlatMapFunction<Edge<Integer, Integer>, Tuple2<Integer, JaccardSketch>> {

    int size = 0;
    HashFuctionList hashlist = null;

    public JCFlatMapper(int size) {
        this.size = size;
        hashlist= new HashFuctionList(size);
    }

    @Override
    public void flatMap(Edge<Integer, Integer> t, Collector<Tuple2<Integer, JaccardSketch>> clctr) throws Exception {

        JaccardSketch u = new JaccardSketch(size);
        JaccardSketch v = new JaccardSketch(size);

        for (int i = 0; i < size; i++) {
            u.addData(i, t.f1, hashlist.hash(t.f1, i));
            v.addData(i, t.f0, hashlist.hash(t.f0, i));
        }
        Tuple2<Integer, JaccardSketch> U = new Tuple2<>(t.f0, u);
        Tuple2<Integer, JaccardSketch> V = new Tuple2<>(t.f1, v);
        clctr.collect(U);
        clctr.collect(V);

    }
}

class JCReducer implements ReduceFunction<Tuple2<Integer, JaccardSketch>> {

    int size = 0;

    public JCReducer(int size) {
        this.size = size;
    }

    @Override
    public Tuple2<Integer, JaccardSketch> reduce(Tuple2<Integer, JaccardSketch> t, Tuple2<Integer, JaccardSketch> t1) {

        t.f1.Union(t1.f1);
        return t;

    }
}
