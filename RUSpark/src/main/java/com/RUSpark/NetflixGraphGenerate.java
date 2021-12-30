package com.RUSpark;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixGraphGenerate {

	public static final String COMMA = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	public static void main(String[] args) throws Exception {

		if (args.length < 0) {
			System.err.println("Usage: NetflixGraphGenerate <file>");
			System.exit(1);
		}

		String InputPath = args[0];

		SparkSession spark = SparkSession.builder().appName("NetflixGraphGenerate").getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> cust_table = lines
				.mapToPair(s -> new Tuple2<Tuple2<Integer, Integer>, Integer>(
						new Tuple2<Integer, Integer>(Integer.parseInt(s.split(COMMA)[0]),
								Integer.parseInt(s.split(COMMA)[2])),
						Integer.parseInt(s.split(COMMA)[1])));
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> dualTable = cust_table.join(cust_table)
				.filter(tuple -> tuple._2()._1() < tuple._2()._2());
		JavaRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> dualrdd = dualTable
				.map(tuple -> new Tuple2<>(tuple._2(), tuple._1()));
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> graph = dualrdd.mapToPair(tuple -> new Tuple2<>(tuple._1(), 1));
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> completionGraph = graph.reduceByKey((i1, i2) -> i1 + i2);
		List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = completionGraph.collect();
		for (Tuple2<Tuple2<Integer, Integer>, Integer> tuple : output) {
			int c1 = tuple._1()._1();
			int c2 = tuple._1()._2();
			int val = tuple._2();
			System.out.printf("(%d,%d) %d\n", c1, c2, val);
		}
		spark.stop();
	}

}
