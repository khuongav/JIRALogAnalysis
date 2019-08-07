package main;

import bdcore.batch.batchview.hive.HiveService;
import bdcore.batch.producer.ProducerService;
import bdcore.batch.producer.prediction.ResignationPrediction;
import bdcore.batch.producer.strategy.regression.JavaLogisticRegression;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Main {
  /**
   * Run the program.
   * 
   * @param args : input parameters args[0] : database contains data to process args[1] : database
   *        contains data processed as input args[2] : name of the table contains results
   * @throws SQLException of queries
   * @throws IOException of importing data
   */
  public static void main(String[] args) throws SQLException, IOException {

    String fromDatabase;
    String toDatabase;
    String outputTableName;

    if (args.length < 3) {
      fromDatabase = "jira_vu_1438329020348";
      toDatabase = "resignationdb";
      outputTableName = "resignation";
    } else {
      fromDatabase = args[0];
      toDatabase = args[1];
      outputTableName = args[2];
    }

    SparkConf conf = new SparkConf().setAppName("JIRALogsAnalysis").setMaster("yarn-cluster");
    JavaSparkContext sc = new JavaSparkContext(conf);

    ProducerService producer = new ResignationPrediction();

    producer.prepareData(sc, fromDatabase, toDatabase);

    producer.analyzeData(sc, new JavaLogisticRegression(), toDatabase, outputTableName);

    sc.stop();

  }
}
