package com.ConsumeSparkKafka.com;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkProduceKafka {
	//special initalization
	final private static String brokers = "localhost:9092";
	final private static Properties props = new Properties();
	private static Producer<String, String> producer = null;
	final private static String kafkaTopic = "newbie";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//Kafka Config with kafka client
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ProducerConfig.RETRIES_CONFIG, "3");
		props.put("producer.type", "sync");
		props.put("batch.num.messages", "1");
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
		props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);

		//spark config
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.driver.memory", "4g").setAppName("Produce");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		//data maping to RDD
		JavaRDD<String> rdd = javaSparkContext.textFile("your path file data", 8);
		rdd.foreach(new VoidFunction<String>() {
			
			public void call(String t) throws Exception {
				// TODO Auto-generated method stub
				//produce data to kafka with spark
				producer.send(new ProducerRecord<String, String>(kafkaTopic, t));
				System.out.println(t);
			}
		});
		
		//close kakfka connection produce
		producer.close();

	}

}
