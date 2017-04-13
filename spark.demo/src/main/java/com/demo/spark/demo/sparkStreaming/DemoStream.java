package com.demo.spark.demo.sparkStreaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 
 * demo for spark_streaming
 * 
 * @author lw
 *
 */
public class DemoStream {
	public static void main(String[] args) throws InterruptedException {
	
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]");
		conf.setAppName("test_demo");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		JavaDStream<String> lines2= jssc.textFileStream("hello");
		
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String arg0) throws Exception {
				return Arrays.asList(arg0.split("the man is left behind")).iterator();
			}
		
		});
		
//		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String,Integer>(){
//
//			@Override
//			public Tuple2<String, Integer> call(String s) throws Exception {
				// TODO Auto-generated method stub
//				return new Tuple2<>(s,1);
//			}
			
//		});
 		
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		
	}
}
