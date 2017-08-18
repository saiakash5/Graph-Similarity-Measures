/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ull.Similarity;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on http://www.kdd.org/kdd2016/papers/files/rfp0465-de-stefaniA.pdf.
 */

public class MyExactTriangleCount {
        
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Integer, NullValue> edges = getGraphStream(env);
              //edges.undirected();
             AllWindowedStream<Tuple3<Integer, Integer,TreeSet<Integer>>, TimeWindow> result1 = edges.buildNeighborhood(false).keyBy(0).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
             result1.apply(new demo()).print();
             
             
//             edges.Neighborhood().keyBy(0).print();
//                      .flatMap(new SumAndEmitCounters1())
//                      .keyBy(0).flatMap(new SumAndEmitCounters2())
//                      .print();


                
                
//              edges.buildNeighborhood(false).map(new ProjectCanonicalEdges()).print();
//              SingleOutputStreamOperator<TreeMap<Integer, TreeSet<Integer>>> result4 = edges.Neighborhood().map(new VertexBiasedSketch());
//              edges.buildNeighborhood(fileOutput).print();
            //DataStreamSink<Edge<Integer, NullValue>> cd = edges.getEdges().print();
            
             //result1.print();
//           DataStreamSink<TreeMap<Integer, ArrayList<String>>> result4 =
//				edges.buildJacard().flatMap(new ExactNeighborhood()).print();
//           edges.buildJacard().print();
//           edges.buildNeighborhood(fileOutput).print();
           //DataStreamSink<Tuple2<Integer, Integer>> result1 =edges.Neighborhood().print();
           //DataStreamSink<TreeMap<Integer, TreeSet<Integer>>> result2 =edges.Neighborhood().flatMap(new VertexBiasedSketch()).print();
             // result4.print();
            DataStream<Tuple2<Integer, Integer>> resultN =
				edges.buildNeighborhood(false)
				.map(new ProjectCanonicalEdges())
				.keyBy(0, 1).flatMap(new IntersectNeighborhoods());
//            
//		DataStream<Tuple2<Integer, Integer>> result =
//				edges.buildNeighborhood(false)
//				.map(new ProjectCanonicalEdges())
//				.keyBy(0, 1).flatMap(new IntersectNeighborhoods())
//				.keyBy(0).flatMap(new SumAndEmitCounters());
//                SingleOutputStreamOperator<Tuple3<Integer, Integer, TreeSet<Integer>>> result1 =
//				(SingleOutputStreamOperator<Tuple3<Integer, Integer, TreeSet<Integer>>>) edges.buildNeighborhood(true);
               // result1.print();
//		if (resultPath != null) {
//			result.writeAsText(resultPath);
//		}
//		else {
//			//result.print();
//		}

		env.execute("Exact Triangle Count");
	}
        
       private static class ReducerFunction implements ReduceFunction<Tuple2<Integer, TreeSet<Integer>>> 
       {
        Map<Tuple2<Integer,Integer>,Integer> final1 = new HashMap<>(); 
        int u = 3;
        int v = 5;
        TreeSet<Integer> source  ;
        TreeSet<Integer> destination ;
        TreeMap<Integer,TreeSet<Integer>> tm = new TreeMap<>();
        @Override
        public Tuple2<Integer, TreeSet<Integer>> reduce(Tuple2<Integer, TreeSet<Integer>> t1, Tuple2<Integer, TreeSet<Integer>> t2) throws Exception
        { 
//            int x;
//            x=-1;
//            source = new TreeSet<>();
//            if(t1.f0 == u &&t2.f0 == v)
//            {
//                x=u;
//                source = t1.f1;
//                destination = t2.f1;
//            }
            
            
               
            
            return t1;
//            int e1 = t.f0;
//            String parts[] = t.f1.split("@");
//            int part1 = Integer.parseInt(parts[0]);
//            int part2 = Integer.parseInt(parts[1]);
//            Tuple2<Integer,Integer> check = new Tuple2<>(e1,part1);
//            
//            if(final1.containsKey(check))
//            {
//               int check1 = final1.get(check);
//               if(check1+part2 == 0)
//               {
//                   final1.remove(check);
//               }
//               
//            }
//            else
//            {
//                final1.put(check, part2);
//            }
            
//            if(!final1.isEmpty())
//            {
//                for(Tuple2<Integer,Integer> tp : final1.keySet())
//                {
//                    if(final1.get(tp)!=-1)
//                    {
//                        
//                    }
//                }
//            }
            
        }

        
    }
        
       
	// *** Transformation Methods *** //

	/**
	 * Receives 2 tuples from the same edge (src + target) and intersects the attached neighborhoods.
	 * For each common neighbor, increase local and global counters.
	 */
	public static final class IntersectNeighborhoods implements
			FlatMapFunction<Tuple3<Integer, Integer, TreeSet<Integer>>, Tuple2<Integer, Integer>> {

		Map<Tuple2<Integer, Integer>, TreeSet<Integer>> neighborhoods = new HashMap<>();

		public void flatMap(Tuple3<Integer, Integer, TreeSet<Integer>> t, Collector<Tuple2<Integer, Integer>> out) {
			//intersect neighborhoods and emit local and global counters
			Tuple2<Integer, Integer> key = new Tuple2<>(t.f0, t.f1);
			if (neighborhoods.containsKey(key)) {
				// this is the 2nd neighborhood => intersect
				TreeSet<Integer> t1 = neighborhoods.remove(key);
				TreeSet<Integer> t2 = t.f2;
				int counter = 0;
				if (t1.size() < t2.size()) {
					// iterate t1 and search t2
					for (int i : t1) {
						if (t2.contains(i)) {
							counter++;
							out.collect(new Tuple2<>(i, 1));
						}
					}
				} else {
					// iterate t2 and search t1
					for (int i : t2) {
						if (t1.contains(i)) {
							counter++;
							out.collect(new Tuple2<>(i, 1));
						}
					}
				}
				if (counter > 0) {
					//emit counter for srcID, trgID, and total
					out.collect(new Tuple2<>(t.f0, counter));
					out.collect(new Tuple2<>(t.f1, counter));
					// -1 signals the total counter
					out.collect(new Tuple2<>(-1, counter));
				}
			} else {
				// first neighborhood for this edge: store and wait for next
				neighborhoods.put(key, t.f2);
			}
		}
	}
        
      
       
       private static class VertexBiasedSketch implements 
               MapFunction<Tuple2<Integer,Integer>,TreeMap<Integer,TreeSet<Integer>>>
       {
           TreeMap<Integer,TreeSet<Integer>> tm1 = new TreeMap<>();
           TreeMap<Integer,TreeSet<Integer>> G_v = new TreeMap<>();
           TreeMap<Integer,TreeSet<String>> reservoir = new TreeMap<>();
           TreeMap<Integer,TreeMap<String,Double>> limits = new TreeMap<>();
           TreeMap<Integer,ArrayList<Integer>> mim_max = new TreeMap<>();
           TreeMap<Integer,String> tm2 = new TreeMap<>();
           int L = 5;
           
           @Override
           public TreeMap<Integer, TreeSet<Integer>> map(Tuple2<Integer, Integer> t) throws Exception  
           {
               
               int u = t.f0;
               int v = t.f1;
               
               TreeMap<Integer,TreeSet<Integer>> cn = new TreeMap<>();
               TreeSet<Integer> source = new TreeSet<>();
               TreeSet<Integer> destination = new TreeSet<>();
               if(!tm1.containsKey(v))
               {
                  TreeSet<Integer> source_edges_v = new TreeSet<>();
                    source_edges_v.add(u);
                    tm1.put(v, source_edges_v); 
                    destination = source_edges_v; 
               }
               if(!tm1.containsKey(u))
                {      
                    TreeSet<Integer> source_edges_u = new TreeSet<>();
                    source_edges_u.add(v);
                    tm1.put(u, source_edges_u);
                    source = source_edges_u;
                }
               if(tm1.containsKey(u))
               {                                         
                    TreeSet<Integer> s_u = tm1.get(u);
                    
//                    clctr.collect(tm1);
                    if(!(s_u.contains(v)))
                        {
                            if(s_u.size()<L)
                            {
                                s_u.add(v);
                                tm1.remove(u);
                                tm1.put(u, s_u);
                            }
                            else
                            {
                                double u_v = (double)(1/v);
                                int min_value = s_u.first();
                                int max_value = s_u.last();
                                if(u_v<=min_value)
                                {
                                    
                                    s_u.remove(max_value);
                                    s_u.add(v);
                                    tm1.replace(u, s_u);
                                }
                            }
                        }
                source = s_u;                     
                }
               if(tm1.containsKey(v))
               {                                         
                    TreeSet<Integer> s_v = tm1.get(v);
                    if(!s_v.contains(u))
                        {
                            if(s_v.size()<L)
                            {
                                s_v.add(u);
                                tm1.remove(v);
                                tm1.put(v, s_v);
                            }
                            else
                            {
                                double u_v = (double)(1/u);
                                int min_value = s_v.first();
                                int max_value = s_v.last();
                                if(u_v<=min_value)
                                {
                                    s_v.remove(max_value);
                                    s_v.add(u);
                                    tm1.replace(v, s_v);
                                }
                            }
                        }
                   destination = s_v;                  
                }
               
               
               
//               if(tm1.containsKey(u)&&tm1.containsKey(v))
//               {
//                    int source_upper = source.last();
//                    int source_lower = source.first();
//                    int destination_upper = destination.last();
//                    int destination_lower = destination.first();
//                    int app_source = Math.round((source_upper+source_lower)/2);
//                    int app_destination = Math.round((destination_upper+destination_lower)/2);
//                    int j=0;
//                    int common_neighbors;
//                    for(int i : source)
//                    {
//                        if(destination.contains(i))
//                        {
//                            j=j+1;
//                        }
//                    }
//                    if(j>0)
//                    {
//                        common_neighbors = j/(Math.max(app_source, app_destination));
//                    }
//                    else
//                    {
//                        common_neighbors = 0;
//                    }
//                    TreeSet<Integer> cn1 = new TreeSet<>();
//                    cn1.add(common_neighbors);
//                    cn.put(u, cn1);
////                    clctr.collect(cn);
//               }
//               else
//               {
//                   TreeSet<Integer> cn1 = new TreeSet<>();
//                   cn1.add(10);
//                   cn.put(u, cn1);
////                   clctr.collect(cn);
//               }
//               clctr.collect(tm1);
                 return tm1;
                
               
//               if(tm1.containsKey(v))
//               {                                         
//                    TreeSet<Integer> s_v = tm1.get(v);
//                    if(!s_v.contains(u))
//                        {
//                            if(s_v.size()<L)
//                            {
//                                s_v.add(u);
//                                tm1.remove(v);
//                                tm1.put(v, s_v);
//                            }
//                            else
//                            {
//                                double u_v = (double)(1/u);
//                                int min_value = s_v.first();
//                                int max_value = s_v.last();
//                                if(u_v<=min_value)
//                                {
//                                    s_v.remove(max_value);
//                                    s_v.add(u);
//                                }
//                            }
//                        }
//                                     
//                }
//                else
//                {      
//                    TreeSet<Integer> source_edges_v = new TreeSet<>();
//                    source_edges_v.add(u);
//                    tm1.put(v, source_edges_v);
//                    
//                }
               
               //clctr.collect(tm1);
               
               
               
               
               
//                   if(tm1.containsKey(v)/*&&reservoir.containsKey(i)8*/)
//                   {
//                      TreeSet<Integer> s_v= tm1.get(v);
//                      if(s_v.contains(u))
//                      {
////                      TreeSet<String> g_v= reservoir.get(i);
//                            if(s_v.size()<L)
//                            {
//                                s_v.add(u);
//                                double gv = (double)(1/u);
////                          g_v.add(Integer.toString(source)+ "@" +Double.toString(gv));
//                                tm1.replace(v, s_v);
////                          reservoir.replace(i, g_v);
//                            }
//                      else
//                      {
//                            double u_v = (double)(1/u);
////                          TreeMap<String,Double> min_max_value = limits.get(i);
////                          double min_value = min_max_value.get("min");
////                          double max_value = min_max_value.get("max");
//                            int min_value = s_v.first();
//                            int max_value = s_v.last();
//                            if(u_v<=min_value)
//                            {
//                                s_v.remove(max_value);
//                                s_v.add(u);
//                              
//                             // min
//                            }
//                        }
//                      }
//                   }
//                   else
//                   {
//                       TreeSet<Integer> ts = new TreeSet<>();
//                       ts.add(source);
//                       tm1.put(i,ts);
//                   }
              
               
               
               
               
               

           }
       }
	/**
	 * Sums up and emits local and global counters.
	 */
	public static final class SumAndEmitCounters implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		Map<Integer, Integer> counts = new HashMap<>();

		public void flatMap(Tuple2<Integer, Integer> t, Collector<Tuple2<Integer, Integer>> out) {
			if (counts.containsKey(t.f0)) {
				int newCount = counts.get(t.f0) + t.f1;
				counts.put(t.f0, newCount);
				out.collect(new Tuple2<>(t.f0, newCount));
			} else {
				counts.put(t.f0, t.f1);
				out.collect(new Tuple2<>(t.f0, t.f1));
			}
		}
	}

	public static final class ProjectCanonicalEdges implements
			MapFunction<Tuple3<Integer, Integer, TreeSet<Integer>>, Tuple3<Integer, Integer, TreeSet<Integer>>> {
		@Override
		public Tuple3<Integer, Integer, TreeSet<Integer>> map(Tuple3<Integer, Integer, TreeSet<Integer>> t) {
			int source = Math.min(t.f0, t.f1);
			int trg = Math.max(t.f0, t.f1);
			t.setField(source, 0);
			t.setField(trg, 1);
			return t;
		}
	}
        
     
        public static final class SumAndEmitCounters1 implements FlatMapFunction<Tuple4<Integer,TreeSet<Integer> ,Integer,TreeSet<Integer>>, Tuple2<Integer, TreeSet<Integer>>> {
		Map<Integer, TreeSet<Integer>> counts = new HashMap<>();
                Map<Integer, Integer> count = new HashMap<>();
		public void flatMap(Tuple4<Integer, TreeSet<Integer>, Integer, TreeSet<Integer>> t, Collector<Tuple2<Integer, TreeSet<Integer>>> out) throws Exception {
                    int i;
			if (counts.containsKey(t.f0)&&t.f1!=null) {
                                int size = count.get(t.f0);
				TreeSet<Integer> newC1 = counts.get(t.f0);
                                TreeSet<Integer> newC2 = t.f1;    
				counts.put(t.f0, newC2);
                                if(size<newC2.size())
                                {
                                count.put(t.f0,newC2.size() );
                                
                                out.collect(new Tuple2<>(t.f0, newC2));
                                }
				
			} else if((t.f1)!=null) {
				counts.put(t.f0, t.f1);
				out.collect(new Tuple2<>(t.f0, t.f1));
                                count.put(t.f0, t.f1.size());
			}
                        if(counts.containsKey(t.f2)&&t.f3!=null)
                        {
                            int size = count.get(t.f2);
                            TreeSet<Integer> newC1 = counts.get(t.f2);
                            TreeSet<Integer> newC2 = t.f3;
    
				counts.put(t.f2, newC2);
                                out.collect(new Tuple2<>(t.f2, newC2));
                                if(size<newC2.size())
                                {
                                count.put(t.f2,newC2.size() );
                                
                                out.collect(new Tuple2<>(t.f2, newC2));
                                }
                                
                                
                        }
                        else if(t.f3!=null) {
				counts.put(t.f2, t.f3);
				out.collect(new Tuple2<>(t.f2, t.f3));
                                count.put(t.f2, t.f3.size());
		}
                }}
        
        private static class SumAndEmitCounters2 implements FlatMapFunction<Tuple2<Integer, TreeSet<Integer>>, Map<Integer,TreeSet<Integer>>> {
        Map<Integer,TreeSet<Integer>> final1 = new HashMap<>();
        Map<Integer,TreeSet<Integer>> final2 = new HashMap<>();
        int f0;
        TreeSet<Integer> f1 = new TreeSet<>();
        int f2;
        TreeSet<Integer> f3 = new TreeSet<>();
        
        @Override
        public void flatMap(Tuple2<Integer, TreeSet<Integer>> t, Collector<Map<Integer,TreeSet<Integer>>> out) throws Exception 
        {
            if(final1.containsKey(t.f0)&&t.f0==1)
            {
                final1.replace(t.f0,t.f1);
                
            }
            else
            {
                final1.put(t.f0,t.f1);
            }
            
            
            if(final2.containsKey(t.f0)&&t.f0==3)
            {
                final2.replace(t.f0,t.f1);
                
            }
            else
            {
                final2.put(t.f0,t.f1);
            }
            out.collect(final1);
//            if(!final2.isEmpty()&&!final1.isEmpty())
//            {
//                int count=0;
//                TreeSet<Integer> a1 = final1.get(1);
//                TreeSet<Integer> a2 = final2.get(3);
//                for(int i : a1)
//                {
//                    if(a2.contains(i))
//                    {
//                        count++;
//                    }
//                 
//                }
//                out.collect(new Tuple3<>(1,3,count));
//        }
    }
        }
        
     private static class FindCommonNeighbors implements FlatMapFunction<Tuple4<Integer,TreeSet<Integer>,Integer,TreeSet<Integer>>,Tuple3<Integer,Integer,Integer>> 
     {
        int u=1 ;
        int v=3;
        int check1=0;
        int check2=0;
        TreeSet<Integer> destination_tree  ;
        TreeSet<Integer> source_tree ;
        int source_average;
        int destination_average;
        int common_neighbors=0;
        Tuple3<Integer,Integer,Integer> outTuple3 = new Tuple3<>();
        @Override
        public void flatMap(Tuple4<Integer,TreeSet<Integer>,Integer,TreeSet<Integer>> t, Collector<Tuple3<Integer, Integer, Integer>> clctr) throws Exception 
        {
//            int count=0;
//            u = t.f0;
//            source_tree = t.f1;
//            v = t.f2;
//            destination_tree = t.f3;
//             source_average = (source_tree.first()+source_tree.last())/2;
//            destination_average = (destination_tree.first()+destination_tree.last())/2;
//            for(int i : source_tree)
//                {
//                    if(destination_tree.contains(i))
//                    {
//                        count++;
//                    }
//                    
//                }
//            if(source_average>destination_average)
//                {
//                   common_neighbors = count/source_average; 
//                }
//                else
//                {
//                common_neighbors = count/destination_average;
//                }
//            outTuple3.setField(u, 0);
//                outTuple3.setField(v, 1);
//                outTuple3.setField(count, 2);
//                clctr.collect(outTuple3);
            int count = 0;
            if(u==t.f0)
            {
                source_tree=t.f1;
                int first = source_tree.first();
                int last = source_tree.last();
                source_average = (first+last)/2;
                check1=1;
//                outTuple3.setField(u, 0);
//                outTuple3.setField(v, 1);
//                outTuple3.setField(check1, 2);
//                clctr.collect(outTuple3);
                for(int j :source_tree)
                    {
//                        outTuple3.setField(u, 0);
//                outTuple3.setField(v, 1);
//                outTuple3.setField(j, 2);
//                clctr.collect(outTuple3);
                       
                    }
            } 
            else
            {
                source_tree = new TreeSet<>();
               
            }
            if(v==t.f0)
            {
                destination_tree = t.f1;
                int first = destination_tree.first();
                int last = destination_tree.last();
                destination_average = (first+last)/2;
                check2=1;
                for(int j :destination_tree)
                    {
//                        outTuple3.setField(u, 0);
//                outTuple3.setField(v, 1);
//                outTuple3.setField(j, 2);
//                clctr.collect(outTuple3);
                       
                    }
//                outTuple3.setField(u, 0);
//                outTuple3.setField(v, 1);
//                outTuple3.setField(destination_average, 2);
//                clctr.collect(outTuple3);
                
            }
            else
            {
                destination_tree = new TreeSet<>();
                
            }  
            
//            outTuple3.setField(u, 0);
//                outTuple3.setField(check1, 1);
//                outTuple3.setField(check2, 2);
//                clctr.collect(outTuple3);
           outTuple3.setField(u, 0);
                outTuple3.setField(source_tree.size(), 1);
                outTuple3.setField(destination_tree.size(), 2);
                clctr.collect(outTuple3);
            if(source_tree.size()>0)
            {
                 
                if(destination_tree.size()>0)
                {
//                outTuple3.setField(u, 0);
//                outTuple3.setField(v, 1);
//                outTuple3.setField(common_neighbors, 2);
//                clctr.collect(outTuple3);
//                clctr.collect(outTuple3);
                for(int i : source_tree)
                {
//                    outTuple3.setField(u, 0);
//                outTuple3.setField(v, 1);
//                outTuple3.setField(i, 2);
                clctr.collect(outTuple3);
                    for(int j :destination_tree)
                    {
//                        outTuple3.setField(u, 0);
//                outTuple3.setField(i, 1);
//                outTuple3.setField(j, 2);
//                clctr.collect(outTuple3);
                        if(i==j)
                            count++;
                        
//                        outTuple3.setField(u, 0);
//                outTuple3.setField(v, 1);
//                outTuple3.setField(count, 2);
//                clctr.collect(outTuple3);
                    }
                    
                }
                if(source_average>destination_average)
                {
                   common_neighbors = count/source_average; 
                }
                else
                {
                common_neighbors = count/destination_average;
                }
                outTuple3.setField(u, 0);
                outTuple3.setField(v, 1);
                outTuple3.setField(common_neighbors, 2);
                clctr.collect(outTuple3);
            }
            }
           
        }

        
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
	private static SimpleEdgeStream<Integer, NullValue> getGraphStream(StreamExecutionEnvironment env) {

		if (fileOutput) {
			return new SimpleEdgeStream<>(env.readTextFile(edgeInputPath)
					.flatMap(new FlatMapFunction<String, Edge<Integer, NullValue>>() {
						@Override
						public void flatMap(String s, Collector<Edge<Integer, NullValue>> out) {
							String[] fields = s.split("\\s");
							if (!fields[0].equals("%")) {
								int src = Integer.parseInt(fields[0]);
								int trg = Integer.parseInt(fields[1]);
								out.collect(new Edge<>(src, trg, NullValue.getInstance()));
							}
						}
					}), env);
		}

		return new SimpleEdgeStream<>(env.fromElements(
				new Edge<>(1, 2, NullValue.getInstance()),
				new Edge<>(2, 3, NullValue.getInstance()),
				new Edge<>(2, 6, NullValue.getInstance()),
				new Edge<>(5, 6, NullValue.getInstance()),
				new Edge<>(1, 4, NullValue.getInstance()),
				new Edge<>(5, 3, NullValue.getInstance()),
				new Edge<>(3, 4, NullValue.getInstance()),
				new Edge<>(3, 6, NullValue.getInstance()),
                                new Edge<>(5, 7, NullValue.getInstance()),
				new Edge<>(1, 3, NullValue.getInstance())), env);
	}

    private static class demo implements AllWindowFunction<Tuple3<Integer,Integer, TreeSet<Integer>>, String, TimeWindow> 
    {
        @Override
        public void apply(TimeWindow w, Iterable<Tuple3<Integer, Integer,TreeSet<Integer>>> values, Collector<String> out) throws Exception 
        {
            Scanner in  = new Scanner(System.in);
            int u;
            int v;
            System.out.println("Enter source");
            u=in.nextInt();
            System.out.println("Enter destination");
            v=in.nextInt();
            
            int count =0;
            int source_average=0;
            int destination_average = 0;
            int destination_max=0;
            String st = new String() ;
            TreeSet<Integer> source = new TreeSet<>();
            TreeSet<Integer> destination = new TreeSet<>();
            for(Tuple3<Integer,Integer, TreeSet<Integer>> value : values)
            {
                if(value.f0==u)
                {
                    source = value.f2;
                }
                if(value.f0==v)
                {
                    destination = value.f2;
                }
            }
            int sum ;
            source_average=(source.first()+source.last())/2;
            destination_average=(destination.first()+destination.last())/2;
            if(source_average>destination_average)
            {
                sum= source_average;
            }
            else
            {
                sum = destination_average;
            }
            for(Integer it : source)
            {
                if(destination.contains(it))
                {
                    count++;
                }
            }
            double similarity = (double)count/sum;
            out.collect(Double.toString(similarity));
            
        }
    }

    

    

    

    

    

    

    

    
}
