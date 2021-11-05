package com.RUSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {

	    if (args.length < 1) {
	      System.err.println("Usage: NetflixGraphGenerate <file>");
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
	  		.add("rating", "integer")
	  		.add("date", "string");
		
		Dataset<Row> df = spark.read().schema(schema).csv(InputPath);
		
		// (movie, rating) : customer_id
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> similars = df.toJavaRDD().mapToPair(new PairFunction<Row, Tuple2<Integer, Integer>, Integer>() {
	    	public Tuple2<Tuple2<Integer, Integer>, Integer> call(Row row) throws Exception {
	    		Integer movie_id = (Integer)row.get(0);
	    		Integer rating = (Integer)row.get(2);
	    		Integer customer_id = (Integer)row.get(1);
	    		Tuple2<Integer, Integer> pair = new Tuple2<>(movie_id, rating);
	    		return new Tuple2<Tuple2<Integer, Integer>, Integer>(pair, customer_id);
	    	}
	    });
		
		Function<Integer, List<Integer>> createCombiner = new Function<Integer, List<Integer>>() {
			@Override
			public List<Integer> call(Integer val) throws Exception {
				List<Integer> lst = new ArrayList<Integer>();
				lst.add(val);
				return lst;
			}
		};
		
		Function2<List<Integer>, Integer, List<Integer>> mergeValue = new Function2<List<Integer>, Integer, List<Integer>>() {
			@Override
			public List<Integer> call(List<Integer> customers, Integer val) throws Exception {
				customers.add(val);
				return customers;
			}
		};
		
		Function2<List<Integer>, List<Integer>, List<Integer>> mergeCombiners = new Function2<List<Integer>, List<Integer>, List<Integer>>() {
			@Override
			public List<Integer> call(List<Integer> lst1, List<Integer> lst2) {
				List<Integer> combined = new ArrayList<Integer>();
				combined.addAll(lst1);
				combined.addAll(lst2);
				return combined;
			}
		};
		
		// (movie, rating) : (customer_id0, customer_id1, ...)
		JavaPairRDD<Tuple2<Integer, Integer>, List<Integer>> pairWithCustomerList = similars.combineByKey(createCombiner, mergeValue, mergeCombiners);
		/*
		List<Tuple2<Tuple2<Integer, Integer>, List<Integer>>> test = pairWithCustomerList.collect();
		for (Tuple2<Tuple2<Integer, Integer>, List<Integer>> tuple : test) {
			
			System.out.println(String.format("%s %s", tuple._1().toString(), tuple._2().toString()));
		}
		*/
		JavaRDD<List<Integer>> customerLists = pairWithCustomerList.values();	
		/*
		List<List<Integer>> test = customerLists.collect();
		for(List<Integer> abc : test) {
			System.out.println(abc.toString());
		}
		*/
		ArrayList<Tuple2<Integer, Integer>> edges = new ArrayList<>();
		
		List<List<Integer>> cusLists = customerLists.collect();
		for(List<Integer> lst : cusLists) {
		Collections.sort(lst);
			for(int i=0; i<lst.size(); i++) {
				for(int j=i+1; j<lst.size(); j++) {
					edges.add(new Tuple2<Integer, Integer>(lst.get(i), lst.get(j)));
				}
			}
		}

		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		JavaRDD<Tuple2<Integer, Integer>> edgesRdd = sc.parallelize(edges);
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> ones = edgesRdd.mapToPair(s -> new Tuple2<Tuple2<Integer, Integer>, Integer>(s, 1));
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> weights = ones.reduceByKey((i1, i2) -> i1 + i2);
		List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = weights.collect();
		
		for (Tuple2<Tuple2<Integer, Integer>, Integer> tuple : output) {
			
			System.out.println(String.format("%s %d", tuple._1().toString(), tuple._2()));
		}
	    
	    spark.stop();
	}

}
