package com.example.demo.controller;

import com.example.demo.service.SparkService;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import scala.collection.Seq;

import java.io.IOException;


@RestController
@RequestMapping("/spark")
public class SparkController {
    private static final Logger logger = LoggerFactory.getLogger(SparkController.class);

    @Autowired
    private SparkService sparkService;
    @PostMapping("/concat")
    public ResponseEntity<?> readCsv(@RequestBody MultipartFile file,
                                     @RequestParam("column") String col,
                                     @RequestParam("value") String value) throws IOException {

        logger.info("Starting with file reading "+file.getOriginalFilename());

        String res = sparkService.concat(file,col,value);

        logger.info("concated successfully");

        return ResponseEntity.ok(res);

    }


    @PostMapping("/fullmask")
    public ResponseEntity<?> fullMask(@RequestBody MultipartFile file,
                                     @RequestParam("column") String col) throws IOException {

        logger.info("Started fullmasking process "+file.getOriginalFilename());

        String res = sparkService.fullMask(file,col);

        logger.info("full mask applied successfully");

        return ResponseEntity.ok(res);

    }

    @PostMapping("/removeExact")
    public ResponseEntity<?> removeExact(@RequestBody MultipartFile file,
                                         @RequestParam("column") String col,
                                         @RequestParam("value") String value) throws IOException {

        logger.info("started remove exact process");

        String res = sparkService.exactMatch(file,col,value);

        logger.info("removed successfully");

        return ResponseEntity.ok(res);

    }


    @PostMapping("/removeSpecialChar")
    public ResponseEntity<?> removeSpecialChars(@RequestBody MultipartFile file,
                                         @RequestParam("column") String col) throws IOException {

        logger.info("started remove special characters process");

        String res = sparkService.removeSpecialCharacters(file,col);

        logger.info("removed all special characters successfully");

        return ResponseEntity.ok(res);

    }
    @PostMapping("/removeNumbers")
    public ResponseEntity<?> removeNumbers(@RequestBody MultipartFile file,
                                                @RequestParam("column") String col) throws IOException {

        logger.info("started remove numbers process");

        String res = sparkService.removeNumbers(file,col);

        logger.info("removed all numbers successfully");

        return ResponseEntity.ok(res);

    }

    @PostMapping("/removeSelectedCharacters")
    public ResponseEntity<?> removeSelectedChars(@RequestBody MultipartFile file,
                                                 @RequestParam("column") String col,
                                                 @RequestParam("value") String value) throws IOException {

        logger.info("started remove selected characters process");

        String res = sparkService.removeSelectedCharacters(file,col,value);

        logger.info("removed all selected characters successfully");

        return ResponseEntity.ok(res);

    }

    @PostMapping("/partialMask")
    public ResponseEntity<?> partialMask(@RequestBody MultipartFile file,
                                                 @RequestParam("column") String col,
                                                 @RequestParam("length") Integer length) throws IOException {

        logger.info("started remove selected characters process");

        String res = sparkService.partialMask(file,col,length);

        logger.info("removed all selected characters successfully");

        return ResponseEntity.ok(res);

    }






    }
