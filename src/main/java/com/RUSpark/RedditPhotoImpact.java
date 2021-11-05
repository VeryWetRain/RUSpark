package com.RUSpark;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

	    if (args.length < 1) {
	      System.err.println("Usage: RedditPhotoImpact <file>");
	      System.exit(1);
	    }
			
		String InputPath = args[0];
		
		/* Implement Here */ 
	
	    SparkSession spark = SparkSession
	      .builder()
	      .appName("RedditPhotoImpact")
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
	    		return new Tuple2<Integer, Integer>((Integer)row.get(0), (Integer)row.get(4)+(Integer)row.get(5)+(Integer)row.get(6) );
	    	}
	    });
	    
	    JavaPairRDD<Integer, Integer> reducedScores = scores.reduceByKey((i1, i2) -> i1 + i2);
	    List<Tuple2<Integer, Integer>> output = reducedScores.collect();
	    //JavaPairRDD<Integer, Integer> vk = reducedScores.mapToPair(v -> v.swap());
	    //List<Tuple2<Integer, Integer>> swapped = vk.sortByKey(true).collect();
	    
	    
	    for (Tuple2<?,?> tuple : output) {
	        System.out.println(tuple._1() + " " + tuple._2());
	    }
	    
	    spark.stop();
	}

}
