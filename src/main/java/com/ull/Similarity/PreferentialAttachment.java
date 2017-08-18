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
public class PreferentialAttachment {

    public SingleOutputStreamOperator<Tuple2<Integer,  HyperLogLog>> process(KeyedStream<Edge<Integer, Integer>, Integer> keyed_edge_stream ){
        return keyed_edge_stream.flatMap(new PAflatmap()).keyBy(0).reduce(new PAreducer());
    }
    public static void main(String[] args) throws Exception {
//
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
//        KeyedStream<Edge<Integer, Integer>, Integer> keyed_edge_stream = edge_stream.keyBy(new KeySelector<Edge<Integer, Integer>, Integer>() {
//            @Override
//            public Integer getKey(Edge<Integer, Integer> in) {
//                return (in.f0 + "_" + in.f1).hashCode();
//            }
//
//        });
//        try {
//
//
//            keyed_edge_stream.flatMap(new PAflatmap()).keyBy(0).reduce(new PAreducer()).addSink(new SinkFunction<Tuple3<Integer, Integer, HyperLogLog>>() {
//                int source =5;
//                int destination = 7;
//                @Override
//                public void invoke(Tuple3<Integer, Integer, HyperLogLog> in) throws Exception {
//                    if(testSink.padata.containsKey(in.f0))
//                    {
//                        testSink.padata.replace(in.f0,in.f2);
//                    }
//                    else
//                    {
//                        testSink.padata.put(in.f0,in.f2);
//                    }
//                    if(testSink.padata.containsKey(source)&&testSink.padata.containsKey(destination))
//                    {
//                        System.out.println("("+source+","+destination+")-->"+(testSink.padata.get(source).cardinality())*testSink.padata.get(destination).cardinality());
//                    }
//                }
//            });
//            env.execute("Adamic Adar");
//        }
//        catch (Exception e)
//        {
//
//        }
//        finally {
////            for(int i:testSink.padata.keySet())
////            {
////                System.out.println(i+"->"+testSink.padata.get(i).cardinality());
////            }
//
//
//        }
    }

}

class PAflatmap implements FlatMapFunction<Edge<Integer, Integer>,Tuple2<Integer,HyperLogLog> >
{


    @Override
    public void flatMap(Edge<Integer, Integer> edge, Collector<Tuple2<Integer,HyperLogLog>> collector) throws Exception {

//            System.out.println(edge.f0+","+edge.f1);
        HyperLogLog u = new HyperLogLog(0.03);
        u.offer(edge.f1);
        HyperLogLog v = new HyperLogLog(0.03);
        v.offer(edge.f0);
        Tuple2<Integer,HyperLogLog> U=new Tuple2<>(edge.f0,u);
        Tuple2<Integer,HyperLogLog> V=new Tuple2<>(edge.f1,v);
        collector.collect(U);
        collector.collect(V);
    }
}


class PAreducer implements ReduceFunction<Tuple2< Integer, HyperLogLog>> {
    @Override
    public Tuple2< Integer, HyperLogLog> reduce(Tuple2<Integer, HyperLogLog> t, Tuple2< Integer, HyperLogLog> t1) throws Exception {
        t.f1.addAll(t1.f1);
//            System.out.println(t.f0+"-"+t.f2.cardinality());
        return t;
    }
}