/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ull.Similarity;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on http://www.kdd.org/kdd2016/papers/files/rfp0465-de-stefaniA.pdf.
 */

public class NewCommonNeighbours {
    public SingleOutputStreamOperator<Tuple3<Integer, CommonNeighboursSketch, PriorityHashing>> process(KeyedStream<Edge<Integer, Integer>, Integer> stream)
    {
        return stream.flatMap(new CommonNeighboursFlatMap()).keyBy(0).reduce(new CommonNeighboursReducer());
    }
        
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


        keyed_edge_stream.flatMap(new CommonNeighboursFlatMap()).keyBy(0).reduce(new CommonNeighboursReducer()).writeAsText("/Users/Akash/Desktop/common neighbours1.txt",FileSystem.WriteMode.OVERWRITE);
		env.execute("Adamic Adar");
	}
        
        static class CommonNeighboursFlatMap implements FlatMapFunction<Edge<Integer, Integer>, Tuple3<Integer, CommonNeighboursSketch,PriorityHashing>>  {

        int size = 0;
        PriorityHashing ph = new PriorityHashing();
        public CommonNeighboursFlatMap(int size) {
        this.size = size;
         }

        private CommonNeighboursFlatMap()
        {
            this(10);

        }
        @Override
        public void flatMap(Edge<Integer, Integer> t, Collector<Tuple3<Integer, CommonNeighboursSketch,PriorityHashing>> clctr) throws Exception {
            
            CommonNeighboursSketch u = new CommonNeighboursSketch(size);
            CommonNeighboursSketch v = new CommonNeighboursSketch(size);


            ph.start(t.f1,t.f0);
//            ph.insert(t.f0,t.f1);

            u.setData(t.f1);
            v.setData(t.f0);

            CountMinSketch cm = new CountMinSketch(0.001,0.99,(int)Math.random());

//            u.addData(t.f0, t.f1,ph);
//            v.addData(t.f1, t.f0,ph);
            
            

            Tuple3<Integer,CommonNeighboursSketch,PriorityHashing> U = new Tuple3<>(t.f0,u,ph);
            Tuple3<Integer,CommonNeighboursSketch,PriorityHashing> V = new Tuple3<>(t.f1,v,ph);

            clctr.collect(U);
            clctr.collect(V);

            

        }
        
        
   

        

        
    }
        
        
        
      static class CommonNeighboursReducer implements ReduceFunction<Tuple3<Integer, CommonNeighboursSketch,PriorityHashing>> {
        HyperLogLog hp = new HyperLogLog(0.03);

        public CommonNeighboursReducer() {
        }

        @Override
        public Tuple3<Integer, CommonNeighboursSketch,PriorityHashing> reduce(Tuple3<Integer, CommonNeighboursSketch,PriorityHashing> t, Tuple3<Integer, CommonNeighboursSketch,PriorityHashing> t1) throws Exception {
            TreeMap<String,Integer> degree = new TreeMap<>();

//            return t;
            hp.offer(t.f0);
            hp.offer(t.f1);
            System.out.println(""+hp.cardinality());
            int t_value = t.f0;
            int t1_value= t1.f0;
            CommonNeighboursSketch c1 = t.f1;
            CommonNeighboursSketch c2 = t1.f1;
            TreeSet<Integer> ts=c2.getData();
            for(int i: ts)
                c1.addData(t_value,i,t.f2);
//            c.addData(t_value,t1_value,t1.f2);
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



    

    
}
