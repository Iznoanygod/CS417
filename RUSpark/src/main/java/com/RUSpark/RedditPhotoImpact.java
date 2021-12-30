package com.RUSpark;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditPhotoImpact {

	public static final String COMMA = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: RedditPhotoImpact <file>");
			System.exit(1);
		}

		String InputPath = args[0];

		SparkSession spark = SparkSession.builder().appName("RedditPhotoImpact").getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		JavaPairRDD<Integer, Integer> indiv = lines
				.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(COMMA)[0]), Integer.parseInt(s.split(COMMA)[4])
						+ Integer.parseInt(s.split(COMMA)[5]) + Integer.parseInt(s.split(COMMA)[6])));

		JavaPairRDD<Integer, Integer> counts = indiv.reduceByKey((i1, i2) -> i1 + i2);

		List<Tuple2<Integer, Integer>> output = counts.collect();
		
		for (Tuple2<Integer, Integer> tuple : output) {
			System.out.println(tuple._1() + " " + tuple._2());
		}
		spark.stop();
	}

}
