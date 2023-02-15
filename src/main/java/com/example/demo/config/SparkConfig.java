//package com.example.demo.config;
//
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.SparkSession;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration
//public class SparkConfig {
//
//    @Bean
//    private SparkConf conf() {
//        return new SparkConf().setAppName("spark-data-mapping").setMaster("local");
//    }
//    @Bean
//    private JavaSparkContext sc() {
//        return  new JavaSparkContext(conf());
//    }
//}
