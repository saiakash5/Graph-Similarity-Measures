/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ull.Similarity;

import java.util.Random;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * @author Akash
 */
public class Utility {
    
        public  static double HashFunction1(int y,int k)
        {
            Random r1 = new Random();
            r1.setSeed((long) (+1*Math.pow(k,2+k)*1000));
            double hash_z = (double)1-(r1.nextDouble()/y);
            
            return hash_z;
        }
        

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String resultPath = null;

	public static boolean parseParameters(String[] args) {

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
	public  static SimpleEdgeStream<Integer, Integer> getGraphStream(StreamExecutionEnvironment env) 
        {
            return new SimpleEdgeStream<>(env.readTextFile("/Users/Akash/Documents/main_drive/ull_notes/csce649-2/dataset/Aakash/graph_1994_1996.txt")
		.flatMap(new FlatMapFunction<String, Edge<Integer, Integer>>() 
                {
                    @Override
                    public void flatMap(String s, Collector<Edge<Integer, Integer>> out) 
                    {
			String[] fields = s.split("\t");
			if (!fields[0].equals("%")) 
                        {
							String u = fields[0];
							String v = fields[1];
							int y=0;
							StringBuilder sbu = new StringBuilder(u);
							sbu.deleteCharAt(0);
							StringBuilder sbv = new StringBuilder(v);
							sbv.deleteCharAt(0);
                            int src = Integer.parseInt(sbu.toString());
                            int trg = Integer.parseInt(sbv.toString());
                            out.collect(new Edge<>(src, trg,1));
			}
                    }
            }), env);
	}

}
