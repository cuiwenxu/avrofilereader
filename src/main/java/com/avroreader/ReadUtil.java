package com.avroreader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

public class ReadUtil {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("SparkReadAvroTest");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlCon = new SQLContext(sc);
        sqlCon.sparkContext().hadoopConfiguration().set("avro.mapred.ignore.inputs.without.extension", "false");
        ///Users/cuiwenxu/Documents/restore/icebergfiles/testupsert/metadata/snap-7348347678694955289-1-4329c3ed-a11c-4538-aae7-662fffff7919.avro
//        Dataset dataset = sqlCon.read().format("com.databricks.spark.avro").load("/Users/cuiwenxu/Documents/restore/icebergfiles/testupsert/metadata/snap-7348347678694955289-1-4329c3ed-a11c-4538-aae7-662fffff7919.avro");
        Dataset dataset = sqlCon.read().format("com.databricks.spark.avro").load("/Users/cuiwenxu/Documents/restore/icebergfiles/testupsert/metadata/4329c3ed-a11c-4538-aae7-662fffff7919-m1.avro");
//        Dataset dataset = sqlCon.read().format("com.databricks.spark.avro").load("/Users/cuiwenxu/Documents/restore/icebergfiles/testupsert/metadata/4329c3ed-a11c-4538-aae7-662fffff7919-m1.avro");
//        Dataset dataset = sqlCon.read().format("com.databricks.spark.avro").load("/Users/cuiwenxu/Documents/restore/icebergfiles/testupsert/metadata/snap-7348347678694955289-1-4329c3ed-a11c-4538-aae7-662fffff7919.avro");
        dataset.toDF().show(1000,false);
    }


}
