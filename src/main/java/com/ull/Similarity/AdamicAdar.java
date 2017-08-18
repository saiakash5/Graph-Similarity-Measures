package com.ull.Similarity;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * Created by Akash on 4/9/17.
 */
public class AdamicAdar {

    public SingleOutputStreamOperator<Tuple2<Integer,  HyperLogLog>> process(KeyedStream<Edge<Integer, Integer>, Integer> keyed_edge_stream ){
        return keyed_edge_stream.flatMap(new AAflatmap()).keyBy(0).reduce(new AAreducer());
    }


}
/**
 * FlatMapFunction
 * Edge<Integer,Integer></> This is an edge tuple which consists of two vertices
 * Tuple2<Integer,HyperLogLog> This flatMapFunction generates a vertex and its HyperLogLog object
 */


class AAflatmap implements FlatMapFunction<Edge<Integer, Integer>,Tuple2<Integer,HyperLogLog> >
{


    @Override
    public void flatMap(Edge<Integer, Integer> edge, Collector<Tuple2<Integer,HyperLogLog>> collector) throws Exception {


        HyperLogLog u = new HyperLogLog(0.03); //Create HyperLogLog object for each vertex and input feed in other vertex
        u.offer(edge.f1);
        HyperLogLog v = new HyperLogLog(0.03);
        v.offer(edge.f0);
        Tuple2<Integer,HyperLogLog> U=new Tuple2<>(edge.f0,u);
        Tuple2<Integer,HyperLogLog> V=new Tuple2<>(edge.f1,v);
        collector.collect(U);
        collector.collect(V);
    }
}


class AAreducer implements ReduceFunction<Tuple2< Integer, HyperLogLog>> {
    @Override
    public Tuple2< Integer, HyperLogLog> reduce(Tuple2<Integer, HyperLogLog> t, Tuple2< Integer, HyperLogLog> t1) throws Exception {
        t.f1.addAll(t1.f1);
//            System.out.println(t.f0+"-"+t.f2.cardinality());
        return t;
    }
}