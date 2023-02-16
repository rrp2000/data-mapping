package com.example.demo.service;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.*;

@Service
public class SparkService {

    Logger logger = LoggerFactory.getLogger(SparkService.class);

    @Autowired
    private SparkSession spark;
    public String storeTheFile(MultipartFile file) throws IOException {
            byte[] bytes = file.getBytes();
            Path path = Paths.get("C:\\Users\\ranja\\Downloads\\data-mapping\\src\\main\\resources\\save\\saved.csv");
            Files.write(path, bytes);
            String convertedFile = "C:\\Users\\ranja\\Downloads\\data-mapping\\src\\main\\resources\\save\\saved.csv";
            return convertedFile;

    }

    public Dataset<Row> getDataset(String convertedFile){
        Dataset<Row> df  = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true").load(convertedFile);
        return df;
    }

    public String concat(MultipartFile file, String col, String value) throws IOException {

            String convertedFile = storeTheFile(file);
            Dataset<Row> df = getDataset(convertedFile);

            df = df.withColumn(col, functions.concat(col(col), functions.concat(lit(value))));

            df.show();
            return "concat successful";

    }

    public String fullMask(MultipartFile file, String col) throws IOException {

            logger.info("getting the file for masking");
            String convertedFile = storeTheFile(file);
            logger.info("converting the file for masking");
            Dataset<Row> df = getDataset(convertedFile);

            df = df.withColumn(col,
                    functions.regexp_replace(df.col(col), ".", "*"));

            df.show();
            return "full mask applied successfully";
    }

    public String exactMatch(MultipartFile file, String col, String value) throws IOException {

            logger.info("getting the file for exact match");

            String convertedFile = storeTheFile(file);

            logger.info("converting for exact match");
            Dataset<Row> df = getDataset(convertedFile);

            df = df.withColumn(
                    col,
                    when(
                            df.col(col).equalTo(value), "")
                            .otherwise(df.col(col)));

            df.show();
            return "removed all fields which mathches "+value+" successfully";

    }

    public String removeSpecialCharacters(MultipartFile file, String col) throws IOException {

        logger.info("getting the file for removing special characters");

        String convertedFile = storeTheFile(file);

        logger.info("converting for removing special characters");
        Dataset<Row> df = getDataset(convertedFile);

        df = df.withColumn(col, functions.regexp_replace(df.col(col), "[^a-zA-Z0-9\\s]", ""));

        df.show();
        return "removed all special characters successfully from column "+col;
    }


    public String removeNumbers(MultipartFile file, String col) throws IOException {

        logger.info("getting the file for removing numbers");

        String convertedFile = storeTheFile(file);

        logger.info("converting for removing numbers");
        Dataset<Row> df = getDataset(convertedFile);

        df = df.withColumn(col, functions.regexp_replace(df.col(col), "\\d+", ""));
        df.show();
        return "removed all numbers successfully from column "+col;
    }

    public String removeSelectedCharacters(MultipartFile file, String col,String value) throws IOException {

        logger.info("getting the file for removing selected character");

        String convertedFile = storeTheFile(file);

        logger.info("converting for removing selected characters");
        Dataset<Row> df = getDataset(convertedFile);

        value = value.replace(",","");

        df = df.withColumn(col, functions.regexp_replace(df.col(col), "["+value+"]", ""));
        df.show();
        return "removed all selected characteers successfully from column "+col;
    }

    public String partialMask(MultipartFile file, String col, Integer length) throws IOException {

        logger.info("getting the file for partial mask");

        String convertedFile = storeTheFile(file);

        logger.info("converting for partial mask");
        Dataset<Row> df = getDataset(convertedFile);

        int maxLength = df.select(functions.max(functions.length(col(col)))).head().getInt(0);


        Column lastFour = substring(col(col), -length, length);

        Column masked = lpad(lit("*"), Math.abs(maxLength-length), "*");

        Column result = functions.concat(masked, lastFour);

        df = df.withColumn(col, result);

        df.show();


        return "Masked partially in column "+col;
    }








}
