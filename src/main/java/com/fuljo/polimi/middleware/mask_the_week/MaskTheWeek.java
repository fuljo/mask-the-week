package com.fuljo.polimi.middleware.mask_the_week;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        /* Disable too verbose logging */
        Logger.getLogger("org").setLevel(Level.OFF);

        /* Check command line parameters */
        if (args.length < 1) {
            System.err.println("Usage: WordCount <file>");
            System.exit(1);
        }
        /* Get file path */
        final String filePath = args[0];

        final SparkConf conf = new SparkConf()
                .setIfMissing("spark.master", "local[1]")
                .setAppName("InvestmentSimulator");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> lines = sc.textFile(filePath);
        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        final JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s,1));
        final JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> out = counts.collect();
        for (Tuple2<?,?> t : out) {
            System.out.println(t._1 + ": " + t._2);
        }

        sc.close();
    }
}
