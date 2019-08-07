package intergrationtest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import bdcore.batch.batchview.hbase.HBaseService;
import bdcore.batch.producer.ProducerService;
import bdcore.batch.producer.prediction.ResignationPrediction;
import bdcore.batch.producer.strategy.regression.JavaLogisticRegression;
import bdcore.infrastructure.common.Constants;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMain {

  String expected1 = "abc,5909123,50384630,\\N,2,25,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.33333334,"
      + "0.0,0.6666667,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,"
      + "0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0,0";

  String expected2 = "def1,0,0,\\N,\\N,5,0.0,0.0,1.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,"
      + "0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,"
      + "0.0,0.0,0.0,0.0,0.0,1";

  @Before
  public void setUp() {}

  @After
  public void tearDown() throws Exception {}

  // @Test
  public void testJiraLogsAnalysis1() throws Exception {

    String fromDatabase;
    String toDatabase;
    String outputTableName;

    fromDatabase = "testdb_in1";
    toDatabase = "testdb_out1";
    outputTableName = "test11";


    SparkConf conf = new SparkConf().setAppName("testJIRALogsAnalysis1").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    ProducerService producer = new ResignationPrediction();

    producer.prepareData(sc, fromDatabase, toDatabase);

    producer.analyzeData(sc, new JavaLogisticRegression(), toDatabase, outputTableName);

    sc.stop();

    HBaseService hbase = new HBaseService("localhost:" + Constants.HBASE_MASTER_PORT);
    hbase.openConnection();

    List<String> columns =
        new ArrayList<>(Arrays.asList("staff", "avg_timespent", "diff_time", "times_sat_sun",
            "times_ten, num_daysoff", "urgent", "high", "normal", "low", "verylow", "onhold",
            "issuetype1", "issuetype3", "issuetype5", "issuetype9", "issuetype10", "issuetype11",
            "issuetype12", "issuetype13", "issuetype14", "issuetype15", "issuetype16",
            "issuetype17", "issuetype18", "issuetype19", "issuetype20", "issuetype21",
            "issuetype22", "issuetype23", "issuetype24", "issuetype25", "issuetype26",
            "issuetype27", "issuetype28", "issuetype29", "issuetype30", "issuetype31",
            "issuetype32", "issuetype33", "issuetype34", "issuetype35", "issuetype36",
            "issuetype37", "issuetype38", "issuetype39", "is_off", "predict"));

    String actual1 = "";

    for (String column : columns) {
      actual1 =
          actual1.concat(Bytes.toString(hbase.get(outputTableName, "abc", "content").getValue(
              Bytes.toBytes("content"), Bytes.toBytes(column))));
    }

    assertEquals(expected1, actual1);

  }

  // @Test
  public void testJiraLogsAnalysis2() throws Exception {

    String fromDatabase;
    String toDatabase;
    String outputTableName;

    fromDatabase = "testdb_in2";
    toDatabase = "testdb_out2";
    outputTableName = "test12";


    SparkConf conf = new SparkConf().setAppName("testJIRALogsAnalysis2").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    ProducerService producer = new ResignationPrediction();

    producer.prepareData(sc, fromDatabase, toDatabase);

    producer.analyzeData(sc, new JavaLogisticRegression(), toDatabase, outputTableName);
    
    sc.stop();

    HBaseService hbase = new HBaseService("localhost:" + Constants.HBASE_MASTER_PORT);
    hbase.openConnection();

    List<String> columns =
        new ArrayList<>(Arrays.asList("staff", "avg_timespent", "diff_time", "times_sat_sun",
            "times_ten, num_daysoff", "urgent", "high", "normal", "low", "verylow", "onhold",
            "issuetype1", "issuetype3", "issuetype5", "issuetype9", "issuetype10", "issuetype11",
            "issuetype12", "issuetype13", "issuetype14", "issuetype15", "issuetype16",
            "issuetype17", "issuetype18", "issuetype19", "issuetype20", "issuetype21",
            "issuetype22", "issuetype23", "issuetype24", "issuetype25", "issuetype26",
            "issuetype27", "issuetype28", "issuetype29", "issuetype30", "issuetype31",
            "issuetype32", "issuetype33", "issuetype34", "issuetype35", "issuetype36",
            "issuetype37", "issuetype38", "issuetype39", "is_off", "predict"));

    String actual2 = "";

    for (String column : columns) {
      actual2 =
          actual2.concat(Bytes.toString(hbase.get(outputTableName, "def1", "content").getValue(
              Bytes.toBytes("content"), Bytes.toBytes(column))));
    }

    hbase.closeConnection();

    assertEquals(expected2, actual2);
  }

}
