package com.ConsumeSparkKafka.com;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkStreamingConsumeKafka {
	public static void main(String[] args) throws InterruptedException {
		// config spark
		SparkConf sparkConf = new SparkConf().setAppName("consume-kafka").setMaster("local[2]")
				.set("spark.driver.memory", "4g");
		
		//kafka topic name map to as list
		String consumerTopic = "newbie";
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(consumerTopic.split(",")));
		
		//HashMap kafka configuration
		HashMap<String, String> kafkaConf = new HashMap<String, String>();
		kafkaConf.put("bootstrap.servers", "localhost:9092");
		kafkaConf.put("auto.offset.reset", "smallest");
		kafkaConf.put("group.id", "TestStream");
		kafkaConf.put("enable.auto.commit", "true");

		//Initialization config Streaming Context and create data stream
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(1000));
		JavaPairInputDStream<String, String> dataStream = KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaConf, topicsSet);

		//Mapping data stream to type Tuple2
		JavaDStream<String> rowStream = dataStream.map(new Function<Tuple2<String, String>, String>() {
			public String call(final Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		//print row data result of data streaming
		rowStream.print();
		//Start Streaming context to always run
		streamingContext.start();
		streamingContext.awaitTermination();
		

	}
}
