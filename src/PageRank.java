
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author Wenhou Lu
 */
public class PageRank {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        //read from input
        JavaRDD file = sc.textFile("hdfs:///input");

        //map by all distinct edges
        JavaRDD followers = file.flatMap(new FlatMapFunction() {
            @Override
            public Iterable call(Object s) {
                return Arrays.asList(((String) s).split("\t"));
            }
        }).distinct().cache();
        double follower_count = followers.count();
        final Broadcast followers_count = sc.broadcast(follower_count);
        JavaPairRDD ranks = followers.mapToPair(new PairFunction() {
            @Override
            public Tuple2 call(Object s) {
                return new Tuple2((String) s, 1.0);
            }
        }).cache();

        //map by edges
        JavaPairRDD pairs = file.mapToPair(new PairFunction() {
            @Override
            public Tuple2 call(Object s) {
                //split each edge into key-value pair where key is follower and value is followee
                String[] parts = ((String) s).split("\t");
                return new Tuple2(parts[0], parts[1]);
            }
        }).distinct().cache();    //group by distinct key and cahce all pairs

        for (int itrs = 0; itrs < 10; itrs++) {
            //iterations
            final Accumulator<Double> dangling = sc.accumulator(0.0);   //an accumulator which saves danglings
            JavaPairRDD iterations = ranks.cogroup(pairs).values()
                    .flatMapToPair(new PairFlatMapFunction() {

                        @Override
                        public Iterable call(Object t) throws Exception {
                            double rank = (double) (Iterables.getOnlyElement((Iterable) (((Tuple2) t)._1)));
                            //get the rank for the current follower
                            if (Iterables.size((Iterable) (((Tuple2) t)._2)) > 0) {
                                double followee_counts = (double) Iterables.size((Iterable) (((Tuple2) t)._2));
                                //get the followee count for the current follower       
                                List<Tuple2> results = new ArrayList<>();
                                for(int i = 0;i<followee_counts;i++){
                                    //create a new key, which is now the followee, not the follower any more
                                    results.add(new Tuple2((Iterables.get((Iterable)(((Tuple2)t)._2), i).toString()), rank / followee_counts));
                                }
                                return results; //return all those Vi with the key Vx, which is the followee
                            } else {
                                dangling.add(rank / (double) followers_count.value());
                                //if the iterable's size is 0, meaning it is a dangling, therefore add up the dangling distribution and store it for later use                               
                            }
                            return null;
                        }

                    }).cache();
            final Broadcast dangling_sum = sc.broadcast((double) (dangling.value()));
            ranks = iterations.reduceByKey(new Function2() {
                @Override
                public Double call(Object a, Object b) {
                    //sum up
                    return (double) a + (double) b;
                }
            }).mapValues(new Function() {
                @Override
                public Double call(Object s) {
                    //give weights and re-calculate the new rank
                    return 0.15 + ((double) s + (double) dangling_sum.value()) * 0.85;
                }
            });
            ranks.cache();
        }

        JavaRDD result = ranks.flatMap(new FlatMapFunction() {
            //adjust output format
            @Override
            public java.lang.Iterable call(Object t) {
                List<String> lines = new ArrayList<String>();
                lines.add(((Tuple2) t)._1 + "\t" + ((Tuple2) t)._2);
                return lines;
            }
        });

        result.saveAsTextFile("hdfs:///pagerank-output");
    }
}
