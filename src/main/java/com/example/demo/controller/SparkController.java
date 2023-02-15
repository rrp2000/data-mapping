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
    @PostMapping("/read/{column}/{value}")
    public ResponseEntity<?> readCsv(@RequestBody MultipartFile file,
                                     @PathVariable("column") String col,
                                     @PathVariable("value") String value) throws IOException {

        logger.info("Starting with file reading "+file.getOriginalFilename());

        String res = sparkService.concat(file,col,value);

        logger.warn("something went wrong");
        if(res.equals(null)){
            return new ResponseEntity<>("Something went wrong", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        logger.info("concated successfully");

        return ResponseEntity.ok(res);

    }


    @PostMapping("/fullmask/{column}")
    public ResponseEntity<?> fullMask(@RequestBody MultipartFile file,
                                     @PathVariable("column") String col) throws IOException {

        logger.info("Started fullmasking process "+file.getOriginalFilename());

        String res = sparkService.fullMask(file,col);

        if(res.equals(null)){
            logger.warn("something went wrong");
            return new ResponseEntity<>("Something went wrong", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        logger.info("full mask applied successfully");

        return ResponseEntity.ok(res);

    }


    @PostMapping("/removeExact/{column}/{value}")
    public ResponseEntity<?> removeExact(@RequestBody MultipartFile file,
                                     @PathVariable("column") String col,
                                     @PathVariable("value") String value) throws IOException {

        logger.info("started remove exact process");

        String res = sparkService.exactMatch(file,col,value);

        logger.warn("something went wrong");
        if(res.equals(null)){
            return new ResponseEntity<>("Something went wrong", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        logger.info("removed successfully");

        return ResponseEntity.ok(res);

    }

}
