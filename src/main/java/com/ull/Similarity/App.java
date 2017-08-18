/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ull.Similarity;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 *
 * @author Akash
 */
public class App {
    
    public static void main(String[] args) throws IOException {
        String file = "/Users/Akash/Documents/main_drive/ull_notes/csce649-2/dataset/Aakash/features_1994_1996_1997_1999.tsv";
        PrintStream pw = new PrintStream("/Users/Akash/Documents/main_drive/ull_notes/csce649-2/output/94_96_updated.tsv");
        FileReader f = new FileReader(new File(file));
        BufferedReader bufferedReader = new BufferedReader(f);
        String line="";

            try {

                //System.setOut(new PrintStream("/Users/Akash/Desktop/output.log"));
                if (!Utility.parseParameters(args)) {
                    return;
                }
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(10);
                SimpleEdgeStream<Integer, Integer> edges = Utility.getGraphStream(env);
                DataStream<Edge<Integer, Integer>> edge_stream = edges.getEdges();
                KeyedStream<Edge<Integer, Integer>, Integer> keyed_edge_stream = edge_stream.keyBy(new KeySelector<Edge<Integer, Integer>, Integer>() {
                    @Override
                    public Integer getKey(Edge<Integer, Integer> in) {
                        return (in.f0 + "_" + in.f1).hashCode();
                    }

                });


                //Jacard Coefficient

                new NewJacardCoefficient(50).process(keyed_edge_stream)
                        .addSink(new SinkFunction<Tuple2<Integer, JaccardSketch>>() {

                            @Override
                            public void invoke(Tuple2<Integer, JaccardSketch> in) throws Exception {

//                            if(source==in.f0 ||destination ==in.f0)
//                            {
                                if (!testSink.Jdata.containsKey(in.f0)) {
                                    testSink.Jdata.put(in.f0, in.f1);
                                } else {
                                    testSink.Jdata.remove(in.f0);
                                    testSink.Jdata.put(in.f0, in.f1);
                                }
                            }

//                        }
                        });


                //Preferential Attachment

                new PreferentialAttachment().process(keyed_edge_stream).addSink(new SinkFunction<Tuple2<Integer, HyperLogLog>>() {

                    @Override
                    public void invoke(Tuple2<Integer, HyperLogLog> in) throws Exception {
                        if (testSink.padata.containsKey(in.f0)) {
                            testSink.padata.replace(in.f0, in.f1);
                        } else {
                            testSink.padata.put(in.f0, in.f1);
                        }

                    }
                });


                //Adamic Adar

                new AdamicAdar().process(keyed_edge_stream).addSink(new SinkFunction<Tuple2<Integer, HyperLogLog>>() {

                    @Override
                    public void invoke(Tuple2<Integer, HyperLogLog> in) throws Exception {
                        if (testSink.aadata.containsKey(in.f0)) {
                            testSink.aadata.replace(in.f0, in.f1);
                        } else {
                            testSink.aadata.put(in.f0, in.f1);
                        }

                    }
                });



                //Common Neighbours

                new CommonNeighboursSample1(80).process(keyed_edge_stream).addSink(new SinkFunction<Tuple2<Integer, CommonNeighboursSketch>>() {

                    Double c = 0.0;

                    @Override
                    public void invoke(Tuple2<Integer, CommonNeighboursSketch> t) throws Exception {



//                    if(source==t.f0||destination==t.f0)
//                    {
                        if (testSink.cdata.containsKey(t.f0)) {
                            testSink.cdata.replace(t.f0,t.f1);
                        } else {
                            testSink.cdata.put(t.f0, t.f1);
                        }
//                    }

//
//
////                    out.println(SampleCNSink.data);
////                    out.println("Common Neighbours "+denominator);
//                        System.out.println(testSink.cdata);
//                        System.out.println("Common Neighbours "+denominator);
//
//                    }


                    }
                });


                env.execute();


            } catch (Exception ex) {
                Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
            } finally {

//                pw.println(testSink.Jdata);


                int cnt = 0;
                while ((line = bufferedReader.readLine()) != null  ) {
                    cnt++;
                    String fields[] = line.split("\t");
                    String u = fields[0];
                    String v = fields[1];
                    StringBuilder sbu = new StringBuilder(u);
                    sbu.deleteCharAt(0);
                    StringBuilder sbv = new StringBuilder(v);
                    sbv.deleteCharAt(0);
                    int source = Integer.parseInt(sbu.toString());
                    int destination = Integer.parseInt(sbv.toString());
                    TreeSet<Integer> commonneighbours = new TreeSet<>();
                    //Common Neighbours
                    Double denominator = 0.0;
                    if (testSink.cdata.containsKey(source) && testSink.cdata.containsKey(destination)) {
                        Double common = 0.0;
                        TreeSet<Integer> sourcets = testSink.cdata.get(source).edge_reservoir;

                        TreeSet<Integer> destts = testSink.cdata.get(destination).edge_reservoir;
                        TreeMap<Integer, Double> sourcetm = testSink.cdata.get(source).treeMap;
                        Double maxvalueS = testSink.cdata.get(source).getMaxHashDataTreeMap();
                        Double maxvalueD = testSink.cdata.get(destination).getMaxHashDataTreeMap();
                        Double minvalueS = testSink.cdata.get(source).getMinHashDataTreeMap();
                        Double minvalueD = testSink.cdata.get(destination).getMinHashDataTreeMap();
                        Double sourceValue = (maxvalueS + minvalueS) / 2;
                        Double destinationValue = (minvalueD + maxvalueD) / 2;

                        TreeMap<Integer, Double> desttm = testSink.cdata.get(destination).treeMap;
                        for (int i : sourcets) {
                            if (destts.contains(i)) {
                                common++;
                                commonneighbours.add(i);

                            }
                        }
                        denominator = common / (Double.max(sourceValue, destinationValue));
                    }



                    //Jacard Coefficient
                    Double logvalue = 0.0;
                    Double commonJ = 0.0;

                    //Jacard Coefficient
                    if (testSink.Jdata.containsKey(source) && testSink.Jdata.containsKey(destination)) {

                        TreeMap<Integer, Integer> source1 = testSink.Jdata.get(source).Hash_position;
                        TreeMap<Integer, Integer> destination1 = testSink.Jdata.get(destination).Hash_position;


                        double cntCommon = 0;
                        int size = source1.size();
                        for (int i : source1.keySet()) {
                            Integer value = source1.get(i);
                            Integer value2 = destination1.get(i);

                            if(value.equals(value2)){
                                cntCommon += 1.0;
                            }
                            //System.out.println("Value:"+value+", Value2:"+value2+", Common:"+cntCommon);

                        }

                        commonJ = cntCommon/(double)size;



                        //Adamic Adar

                        for(int i: commonneighbours)
                        {
                            if(testSink.aadata.containsKey(i)) {
                                logvalue = logvalue + ((double) 1 / (double) Math.log(testSink.aadata.get(i).cardinality()));
                            }
                        }


//                        for (int i : source1.keySet()) {
//                            sourceAl.add(source1.get(i));
//                        }
//                        for (int i : destination1.keySet()) {
//                            destAl.add(destination1.get(i));
//                        }
//
//                        for (int i : source1.keySet()) {
//
//                                if (destination1.containsKey(source1.get(i))) {
//                                    if(!commonAl.contains(source1.get(i))) {
//                                        commonAl.add(source1.get(i));
//                                    }
//                                    commonJ++;
//                                    try {
//                                        logvalue = logvalue + ((double) i / Math.log(testSink.padata.get(i).cardinality()));
//                                    }
//                                    catch (Exception e)
//                                    {
//                                        //kkkk
//                                    }
//                                }
//
//                        }
//                        if (commonAl.size() > 0) {
//                            for (int i : commonAl) {
//                                int x = i;
//                                int min = Math.min(Collections.frequency(sourceAl, x), Collections.frequency(destAl, x));
//                                if (min > commonJ)
//                                    commonJ = min;
//                            }
//                        }

                    }
//            System.out.println("similarity->" + commonJ);
//                    PrintStream out = new PrintStream("/Users/Akash/Desktop/output/Jaccard Coefficient.txt");
//
//                    out.print(testSink.Jdata);

                    //Preferential Attachement
                    double pa =0.0;
                    if (testSink.padata.containsKey(source) && testSink.padata.containsKey(destination)) {
//                    System.out.println(source + " " + destination + " " + (testSink.padata.get(source).cardinality()) * testSink.padata.get(destination).cardinality() + " " + commonJ + " " + Math.ceil(denominator) + " " + Math.ceil(logvalue));
                        pa=(testSink.padata.get(source).cardinality()) * testSink.padata.get(destination).cardinality();
                    }
                    pw.println(source + "\t" + destination + "\t" + pa + " \t" + commonJ + "\t" + (denominator) + "\t" + (logvalue));

//            for(int i:testSink.padata.keySet())
////            {
////                System.out.println(i+"->"+testSink.padata.get(i).cardinality());
////            }

                }

            }
        
        pw.close();
                

    }
}
