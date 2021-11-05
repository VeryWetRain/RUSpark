package com.RUSpark;

import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

	    if (args.length < 1) {
	      System.err.println("Usage: NetflixMovieAverage <file>");
	      System.exit(1);
	    }
			
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
			      .builder()
			      .appName("RedditPhotoImpact")
				      .getOrCreate();
			    
	    StructType schema = new StructType()
	    		.add("movie_id", "integer")
	    		.add("customer_id", "integer")
	    		.add("rating", "double")
	    		.add("date", "string");
	    
	    Dataset<Row> df = spark.read().schema(schema).csv(InputPath);
	
	    JavaPairRDD<Integer, Double> ratings = df.toJavaRDD().mapToPair(new PairFunction<Row, Integer, Double>() {
	    	public Tuple2<Integer, Double> call(Row row) throws Exception {
	    		return new Tuple2<Integer, Double>((Integer)row.get(0), (Double)row.get(2));
	    	}
	    });
	    
	    
	    JavaPairRDD<Integer, Tuple2<Double, Double>> withCounts = ratings.mapValues(value -> new Tuple2<Double, Double>(value, 1.0));
	    JavaPairRDD<Integer, Tuple2<Double, Double>> reducedWithCounts = withCounts.reduceByKey(new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {
	    		@Override
	    		public Tuple2<Double, Double> call(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
	    			return new Tuple2<Double, Double>(a._1() + b._1(), a._2() + b._2());
	    		}
	    	});
	
	    JavaPairRDD<Integer, Double> avgs = reducedWithCounts.mapValues(value -> value._1() / value._2());
	    List<Tuple2<Integer, Double>> output = avgs.collect();
	    
	    //JavaPairRDD<Double, Integer> vk = avgs.mapToPair(v -> v.swap());
	    //List<Tuple2<Double, Integer>> swapped = vk.sortByKey(true).collect();
	    
	    for (Tuple2<?,?> tuple : output) {
	    	System.out.println(String.format("%d %.2f", tuple._1(), tuple._2()));
	    }
	
	    spark.stop();
	}
}
