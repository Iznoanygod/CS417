package com.RUSpark;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixMovieAverage {

	public static final String COMMA = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: NetflixMovieAverage <file>");
			System.exit(1);
		}

		String InputPath = args[0];

		SparkSession spark = SparkSession.builder().appName("NetflixMovieAverage").getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> indiv = lines
				.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(COMMA)[0]),
						new Tuple2<>(1, Integer.parseInt(s.split(COMMA)[2]))));

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> counts = indiv
				.reduceByKey((i1, i2) -> new Tuple2<>(i1._1() + i2._1(), i1._2() + i2._2()));

		List<Tuple2<Integer, Tuple2<Integer, Integer>>> output = counts.collect();
		for (Tuple2<Integer, Tuple2<Integer, Integer>> tuple : output) {
			double rating = 1.0 * tuple._2()._2() / tuple._2()._1();
			System.out.printf("%d %.2f\n", tuple._1(), rating);
		}
		spark.stop();
	}

}
