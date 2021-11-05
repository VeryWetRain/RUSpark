package com.RUSpark;

import java.util.List;
import java.util.stream.IntStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {

	    if (args.length < 1) {
	      System.err.println("Usage: RedditHourImpact <file>");
	      System.exit(1);
	    }
			
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
		      .builder()
		      .appName("RedditHourImpact")
			      .getOrCreate();
		
		StructType schema = new StructType()
	    		.add("image_id", "integer")
	    		.add("unixtime", "long")
	    		.add("title", "string")
	    		.add("subreddit", "string")
	    		.add("upvotes", "integer")
	    		.add("downvotes", "integer")
	    		.add("comments", "integer");
	    
	    Dataset<Row> df = spark.read().schema(schema).csv(InputPath);
	    JavaPairRDD<Integer, Integer> scores = df.toJavaRDD().mapToPair(new PairFunction<Row, Integer, Integer>() {
	    	public Tuple2<Integer, Integer> call(Row row) throws Exception {
	    		Date time = new Date((long)row.get(1) * 1000);
	    		int houroffset = time.getHours();
	    		int impact = (Integer)row.get(4)+(Integer)row.get(5)+(Integer)row.get(6);
	    		return new Tuple2<Integer, Integer>((Integer)houroffset, (Integer)impact);
	    	}
	    });
	    
	
	    JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());  
	    Integer keys[] = IntStream.range(0, 24)
	    		.boxed()
	    		.toArray(Integer[]::new);
	    JavaRDD<Integer> hourKeys = sc.parallelize(Arrays.asList(keys));
	    JavaPairRDD<Integer, Integer> zeros = hourKeys.mapToPair(s -> new Tuple2<>(s, 0));
	    JavaPairRDD<Integer, Integer> joined = scores.union(zeros);
	    
	    JavaPairRDD<Integer, Integer> reducedScores = joined.reduceByKey((i1, i2) -> i1 + i2);
	    List<Tuple2<Integer, Integer>> output = reducedScores.sortByKey(true).collect();
	    for (Tuple2<?,?> tuple : output) {
	        System.out.println(tuple._1() + " " + tuple._2());
	      }
	    
	    spark.stop();
	
	}

}
