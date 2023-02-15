package com.example.demo.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

@Service
public class SparkService {

    private static SparkSession spark = SparkSession.builder().master("local").getOrCreate();

    public String storeTheFile(MultipartFile file) throws IOException {
        byte[] bytes = file.getBytes();
        Path path = Paths.get("C:\\Users\\ranja\\Downloads\\data-mapping\\src\\main\\resources\\save.csv");
        Files.write(path, bytes);
        String convertedFile = "C:\\Users\\ranja\\Downloads\\data-mapping\\src\\main\\resources\\save.csv";
        return convertedFile;
    }

    public Dataset<Row> getDataset(String convertedFile){
        Dataset<Row> df  = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true").load(convertedFile);
        return df;
    }

    public String concat(MultipartFile file, String col, String value) throws IOException {
        try{

            String convertedFile = storeTheFile(file);
            Dataset<Row> df = getDataset(convertedFile);

            df = df.withColumn(col, functions.concat(col(col), functions.concat(lit(value))));

            df.show();
            spark.stop();
            return "concat successful";
        }catch (Exception e){
            return null;
        }
    }

    public String fullMask(MultipartFile file, String col) throws IOException {
        try{

            String convertedFile = storeTheFile(file);
            Dataset<Row> df = getDataset(convertedFile);

            df = df.withColumn(col,
                    functions.regexp_replace(df.col(col), ".", "*"));

            df.show();
            spark.stop();
            return "full mask applied successfully";
        }catch (Exception e){
            return null;
        }
    }

    public String exactMatch(MultipartFile file, String col, String value) throws IOException {
        try{

            String convertedFile = storeTheFile(file);
            Dataset<Row> df = getDataset(convertedFile);

            df = df.withColumn(
                    col,
                    functions.when(
                            df.col(col).equalTo(value), "")
                            .otherwise(df.col(col)));

            df.show();
            spark.stop();
            return "removed all fields which mathches "+value+" successfully";
        }catch (Exception e){
            return null;
        }
    }




}
