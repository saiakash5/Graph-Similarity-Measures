package com.ull.Similarity;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.PrintStream;
import java.util.TreeSet;
import java.util.Scanner;
import java.util.TreeMap;

/**
 * Created by Akash on 4/7/17.
 */
public class CommonNeighboursSample1
{
    int size=10;
    public CommonNeighboursSample1(int size)
    {
        this.size = size;
    }
    public DataStream<Tuple2<Integer,CommonNeighboursSketch>> process(KeyedStream<Edge<Integer, Integer>, Integer> keyed_edge_stream)
    {
        return keyed_edge_stream.flatMap(new CommonNeighboursSample1FlatMap(size)).keyBy(0).reduce(new CommonNeighboursSample1Reducer());
    }

    public static void main(String[] args) throws Exception {

//        //System.setOut(new PrintStream("/Users/Akash/Desktop/output.log"));
//        if (!parseParameters(args)) {
//            return;
//        }
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        //SimpleEdgeStream<Integer, Integer> edges = getGraphStream(env);
//        SimpleEdgeStream<Integer, Integer> edges = Utility.getGraphStream(env);
//
//        DataStream<Edge<Integer, Integer>> edge_stream = edges.getEdges();
//        KeyedStream<Edge<Integer, Integer>, Integer> keyed_edge_stream=edge_stream.keyBy(new KeySelector<Edge<Integer, Integer>,Integer>(){
//            @Override
//            public Integer getKey(Edge<Integer, Integer> in)
//            {
//                return (in.f0+"_"+in.f1).hashCode();
//            }
//
//        }); //.timeWindow(Time.milliseconds(10), Time.milliseconds(2));
//
//    try {
//
//
//        keyed_edge_stream.flatMap(new CommonNeighboursSample1FlatMap()).keyBy(0).reduce(new CommonNeighboursSample1Reducer()).addSink(new SinkFunction<Tuple3<Integer, Integer, CommonNeighboursSketch>>() {
//            int source=8;
//            int destination=20;
//            Double c =0.0;
////            PrintStream out = new PrintStream("/Users/Akash/Desktop/sample2 common neighbours1.txt");
//            @Override
//            public void invoke(Tuple3<Integer, Integer, CommonNeighboursSketch> t) throws Exception {
////                Scanner in = new Scanner(System.in);
////                System.out.println("Enter node1");
////                source = in.nextInt();
////                System.out.println("Enter node2");
////                destination = in.nextInt();
//
//
//
//
//                if(source==t.f0||destination==t.f0)
//                {
//                    if(SampleCNSink.data.containsKey(t.f0))
//                    {
//                        SampleCNSink.data.replace(t.f0,t.f2);
//                    }
//                    else
//                    {
//                        SampleCNSink.data.put(t.f0,t.f2);
//                    }
//                }
//                if(SampleCNSink.data.containsKey(source)&&SampleCNSink.data.containsKey(destination)) {
//                    Double common=0.0;
//                    TreeSet<Integer> sourcets = SampleCNSink.data.get(source).edge_reservoir;
//                    TreeSet<Integer> destts = SampleCNSink.data.get(destination).edge_reservoir;
//                    TreeMap<Integer,Double> sourcetm = SampleCNSink.data.get(source).treeMap;
//                    Double maxvalueS = SampleCNSink.data.get(source).getMaxHashDataTreeMap();
//                    Double maxvalueD = SampleCNSink.data.get(destination).getMaxHashDataTreeMap();
//                    Double minvalueS = SampleCNSink.data.get(source).getMinHashDataTreeMap();
//                    Double minvalueD = SampleCNSink.data.get(destination).getMinHashDataTreeMap();
//                    Double sourceValue = (maxvalueS+minvalueS)/2;
//                    Double destinationValue = (minvalueD+maxvalueD)/2;
//
//                    TreeMap<Integer,Double> desttm  = SampleCNSink.data.get(destination).treeMap;
//                    for(int i:sourcets)
//                    {
//                        if(destts.contains(i)) {
//                            common++;
//
//                        }
//                    }
//                    Double denominator =  common/(Double.max(sourceValue,destinationValue));
//
//
////                    out.println(SampleCNSink.data);
////                    out.println("Common Neighbours "+denominator);
//                    System.out.println(SampleCNSink.data);
//                    System.out.println("Common Neighbours "+denominator);
//
//                }
//
//
//            }
//        });//.writeAsText("/Users/Akash/Desktop/sample common neighbours1.txt", FileSystem.WriteMode.OVERWRITE);
//        env.execute("Adamic Adar");
//    }
//    catch (Exception e)
//    {
//
//    }
//    finally {
////        System.out.println(SampleCNSink.data);
////        TreeMap<Integer,CommonNeighboursSketch> tm = SampleCNSink.data;
//
//
//    }
    }

}

class CommonNeighboursSample1Reducer implements org.apache.flink.api.common.functions.ReduceFunction<Tuple2<Integer, CommonNeighboursSketch>> {

    @Override
    public Tuple2<Integer, CommonNeighboursSketch> reduce(Tuple2<Integer, CommonNeighboursSketch> t, Tuple2<Integer, CommonNeighboursSketch> t1) throws Exception {

        CommonNeighboursSketch cm = t.f1;
        cm.Union(t1.f1);

        //cm.SampleAddData(t1.f1);
        return t;

    }
}


class CommonNeighboursSample1FlatMap implements FlatMapFunction<Edge<Integer, Integer>, Tuple2<Integer,CommonNeighboursSketch>> {
    int size=50;
    CommonNeighboursSample1FlatMap(int size)
    {
        this.size=size;
    }

    @Override
    public void flatMap(Edge<Integer, Integer> edge, Collector<Tuple2<Integer,CommonNeighboursSketch>> collector) throws Exception {
        CommonNeighboursSketch u = new CommonNeighboursSketch(size);
        CommonNeighboursSketch v = new CommonNeighboursSketch(size);
        u.initialize(edge.f1);
        v.initialize(edge.f0);
        Tuple2<Integer,CommonNeighboursSketch> U = new Tuple2<>(edge.f0,u);
        Tuple2<Integer,CommonNeighboursSketch> V = new Tuple2<>(edge.f1,v);
        collector.collect(U);
        collector.collect(V);
    }
}