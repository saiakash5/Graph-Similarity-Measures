/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ull.Similarity;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.io.PrintStream;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on http://www.kdd.org/kdd2016/papers/files/rfp0465-de-stefaniA.pdf.
 */

public class NewCommonNeighboursTest {

        
	public static void main(String[] args) throws Exception {

                //System.setOut(new PrintStream("/Users/Akash/Desktop/output.log"));
		if (!parseParameters(args)) {
			return;
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
		//SimpleEdgeStream<Integer, Integer> edges = getGraphStream(env);
        SimpleEdgeStream<Integer, Integer> edges = Utility.getGraphStream(env);

                DataStream<Edge<Integer, Integer>> edge_stream = edges.getEdges();
                KeyedStream<Edge<Integer, Integer>, Integer> keyed_edge_stream=edge_stream.keyBy(new KeySelector<Edge<Integer, Integer>,Integer>(){
                    @Override
                    public Integer getKey(Edge<Integer, Integer> in)
                    {
                        double hash =  HashFunction1(in.f0,in.f1);
                        return (in.f0+"_"+in.f1).hashCode();
                    }

                }); //.timeWindow(Time.milliseconds(10), Time.milliseconds(2));

    try {
        keyed_edge_stream.flatMap(new CommonNeighboursFlatMap()).keyBy(0).reduce(new CommonNeighboursReducer()).addSink(new SinkFunction<Tuple5<Integer, Integer, CommonNeighboursSketch, HyperLogLog, TreeMap<Integer, HyperLogLog>>>() {
            int source = 5;
            int destination = 7;

            @Override
            public void invoke(Tuple5<Integer, Integer, CommonNeighboursSketch, HyperLogLog, TreeMap<Integer, HyperLogLog>> in) throws Exception {

//                if (source == in.f0 || destination == in.f0) {
                    if (!CNTestSink.data.containsKey(in.f0)) {
                        CNTestSink.data.put(in.f0, in.f2);
                    } else {
                        CNTestSink.data.replace(in.f0, in.f2);
                    }

                    if(!CNTestSink.Min_Max.containsKey(in.f0))
                    {
                        CNTestSink.Min_Max.put(in.f0,getMinMax(in.f4,in.f3,in.f2));

                    }
                    else {
                        CNTestSink.Min_Max.replace(in.f0,getMinMax(in.f4,in.f3,in.f2));

                    }

//                    int arr1[] = getMinMax(in.f4,in.f3,in.f2);


                }

//            }
        });
        env.execute("Adamic Adar");
    }
    catch (Exception e)
    {
        System.out.println("Exception--->"+e);
    }
    finally {
//        System.out.println(CNTestSink.data);
        PrintStream ps = new PrintStream("/Users/Akash/Desktop/output/CMTest.txt");
        for(int i:CNTestSink.data.keySet())
        {
            int arr[] = CNTestSink.Min_Max.get(i);
            ps.println(i+"->"+CNTestSink.data.get(i)+"  min_max-> "+arr[0]+" ,"+arr[1]);
        }

//        ps.println(CNTestSink.data);
    }
    }



    static class CommonNeighboursFlatMap implements FlatMapFunction<Edge<Integer, Integer>, Tuple5<Integer,Integer,CommonNeighboursSketch,HyperLogLog,TreeMap<Integer,HyperLogLog>>>  {
	    HyperLogLog total = new HyperLogLog(0.03);
	    TreeMap<Integer,HyperLogLog> degree= new TreeMap<>();
        int size = 0;
        public CommonNeighboursFlatMap(int size) {
        this.size = size;
         }

        private CommonNeighboursFlatMap()
        {
            this(10);

        }
        @Override
        public void flatMap(Edge<Integer, Integer> t, Collector<Tuple5<Integer,Integer,CommonNeighboursSketch,HyperLogLog,TreeMap<Integer,HyperLogLog>>>clctr) throws Exception {
            total.offer(t.f1);
            total.offer(t.f0);
            if(degree.containsKey(t.f0))
            {
                HyperLogLog hpu = degree.remove(t.f0);
                hpu.offer(t.f1);
                degree.put(t.f0,hpu);
            }
            else
            {
                HyperLogLog hpu = new HyperLogLog(0.03);
                hpu.offer(t.f1);
                degree.put(t.f0,hpu);
            }
            if(degree.containsKey(t.f1))
            {
                HyperLogLog hpv = degree.remove(t.f1);
                hpv.offer(t.f0);
                degree.put(t.f1,hpv);
            }
            else
            {
                HyperLogLog hpv = new HyperLogLog(0.03);
                hpv.offer(t.f0);
                degree.put(t.f1,hpv);
            }

            CommonNeighboursSketch u = new CommonNeighboursSketch(size);
            CommonNeighboursSketch v = new CommonNeighboursSketch(size);

            u.initialize(t.f1);
            v.initialize(t.f0);


            Tuple5<Integer,Integer,CommonNeighboursSketch,HyperLogLog,TreeMap<Integer,HyperLogLog>> U = new Tuple5<>(t.f0,t.f1,u,total,degree);
            Tuple5<Integer,Integer,CommonNeighboursSketch,HyperLogLog,TreeMap<Integer,HyperLogLog>> V = new Tuple5<>(t.f1,t.f0,v,total,degree);

            clctr.collect(U);
            clctr.collect(V);

        }
        
        
   

        

        
    }
        
        
        
      static class CommonNeighboursReducer implements ReduceFunction<Tuple5<Integer,Integer,CommonNeighboursSketch,HyperLogLog,TreeMap<Integer,HyperLogLog>>> {
	    int size = 10;

        TreeSet<Integer> ts = new TreeSet<>();

          @Override
          public Tuple5<Integer,Integer,CommonNeighboursSketch,HyperLogLog,TreeMap<Integer,HyperLogLog>> reduce(Tuple5<Integer,Integer,CommonNeighboursSketch,HyperLogLog,TreeMap<Integer,HyperLogLog>> t,Tuple5<Integer,Integer,CommonNeighboursSketch,HyperLogLog,TreeMap<Integer,HyperLogLog>> t1) throws Exception
          {
              CommonNeighboursSketch cm = t.f2;
              cm.add(t1.f1,size,t1.f4,t1.f3);
              return t;
          }
      }

        
        
        public static double HashFunction1(int edge,int degree)
        {

            return (double)1/(double)degree;

        }

         


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String resultPath = null;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 2) {
				System.err.println("Usage: ExactTriangleCount <input edges path> <result path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			resultPath = args[1];
		} else {
			System.out.println("Executing ExactTriangleCount example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: ExactTriangleCount <input edges path> <result path>");
		}
		return true;
	}


	@SuppressWarnings("serial")
	private static SimpleEdgeStream<Integer, Integer> getGraphStream(StreamExecutionEnvironment env) 
        {
            return new SimpleEdgeStream<>(env.readTextFile("/Users/Akash/Documents/main_drive/ull_notes/csce649-2/dataset/set1_example20000.txt")
		.flatMap(new FlatMapFunction<String, Edge<Integer, Integer>>() 
                {
                    @Override
                    public void flatMap(String s, Collector<Edge<Integer, Integer>> out) 
                    {
			String[] fields = s.split("\\s");
			if (!fields[0].equals("%")) 
                        {
                            int src = Integer.parseInt(fields[0]);
                            int trg = Integer.parseInt(fields[1]);
                            out.collect(new Edge<>(src, trg,1));
			}
                    }
            }), env);
	}



    private static int[] getMinMax(TreeMap<Integer, HyperLogLog> degree, HyperLogLog total, CommonNeighboursSketch f2)
    {
        int[] arr = new int[2];
        int min_value=-1;
        int max_value=-1;
        TreeSet<Integer> edge_reservoir = f2.edge_reservoir;
        long min=Long.MAX_VALUE;
        long max=Long.MIN_VALUE;
        long total_edges = total.cardinality();
        for(int i: edge_reservoir)
        {
            HyperLogLog hp = degree.get(i);
            long edge_degree=hp.cardinality();
            long hash = 1-(edge_degree/total_edges);
            if(hash<min) {
                min = hash;
                min_value = i;
            }
            if(hash>max) {
                max = hash;
                max_value = i;
            }
        }
        arr[0] = min_value;
        arr[1] = max_value;
        return arr;
    }

    
}
