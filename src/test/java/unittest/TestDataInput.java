package unittest;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;
import static org.junit.Assert.assertEquals;

import bdcore.batch.batchview.hive.HiveService;
import bdcore.batch.producer.ProducerService;
import bdcore.batch.producer.prediction.ResignationPrediction;
import bdcore.batch.utils.SparkUtil;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class TestDataInput {

  final String pwd = "/home/gcsadmin/workspace/JIRALogsAnalysis/";

  String expectedSpark1 = "abc,0,0,1.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.5555556,0,0,0.44444445,"
      + "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0";

  String expectedSpark2 = "def1,0,0,0.9285714,0,0.071428575,0,0,0,0,0,0,0,0.035714287,0,0,0,"
      + "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.035714287,0.9285714,0,0,0,0,0,0,0,0";

  String expectedPrepareData1 = "ghj,46443,3600000,29,2,47,0.0,"
      + "0.00204081623815,0.997959196568,0.0,0.0,0.0,0.216326534748,"
      + "0.420408159494,0.161224484444,0.00204081623815,0.00612244894728,"
      + "0.0040816324763,0.0224489793181,0.00204081623815,0.0,0.0,"
      + "0.15918366611,0.0,0.0,0.0040816324763,0.0,0.0,0.0,0.0,0.0,0.0,0.0,"
      + "0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.00204081623815,0.0,0.0,0";

  String expectedPrepareData2 = "klm2,95194,15760980,13,2,57,0.0,0.013937282376,0.982578396797,0.0,"
      + "0.003484320594,0.0,0.12195122242,0.797909379005,0.003484320594,0.003484320594,"
      + "0.006968641188,0.0,0.0243902429938,0.0,0.003484320594,0.0104529615492,0.0174216032028,"
      + "0.0,0.0,0.006968641188,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.003484320594,"
      + "0.0,0.0,0.0,0.0,0.0,0.0,0.0,0";

  @Before
  public void setUp() throws SQLException {}

  @After
  public void tearDown() throws Exception {}

  //@Test
  public void testParsePercentageColumn1() throws Exception {
    SparkConf conf = new SparkConf().setAppName("testParsePercentageColumn1").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    String input1 = pwd + "src/test/resources/inputSpark1";
    String output1 = pwd + "/src/test/resources/outputSpark1/";

    deleteDirectory(output1);

    SparkUtil.parsePercentageColumn(sc, "file://" + input1, "file://" + output1);

    @SuppressWarnings("resource")
    String actual1 = new Scanner(new File(output1 + "/part-00000")).next();
    sc.stop();

    assertEquals(expectedSpark1, actual1);
  }

  // @Test
  public void testParsePercentageColumn2() throws Exception {
    SparkConf conf = new SparkConf().setAppName("testParsePercentageColumn2").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    String input2 = pwd + "src/test/resources/inputSpark2";
    String output2 = pwd + "/src/test/resources/outputSpark2/";

    deleteDirectory(output2);

    SparkUtil.parsePercentageColumn(sc, "file://" + input2, "file://" + output2);

    @SuppressWarnings("resource")
    String actual2 = new Scanner(new File(output2 + "/part-00000")).next();
    sc.stop();

    assertEquals(expectedSpark2, actual2);
  }


  //@Test
  public void testPrepareData1() throws Exception {

    SparkConf conf = new SparkConf().setAppName("testPrepareData1").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    String fromDatabase = "testdb_in1";
    String toDatabase = "testdb_out1";
    ProducerService producer = new ResignationPrediction();

    producer.prepareData(sc, fromDatabase, toDatabase);

    HiveService hive = new HiveService("localhost", "10000", toDatabase, "khuongav", "");
    hive.openConnection();
    ResultSet rs = hive.executeQuery("select * from input");

    final int num_col = 47;
    String actual1 = "";
    if (rs.next()) {
      for (int i = 1; i <= num_col; i++) {
        if (i == num_col) {
          actual1 = actual1.concat(rs.getString(i) + ",");
        } else {
          actual1 = actual1.concat(rs.getString(i) + ",");
        }
      }
    }

    hive.closeConnection();
    sc.stop();

    assertEquals(expectedPrepareData1, actual1);

  }

  //@Test
  public void testPrepareData2() throws Exception {

    SparkConf conf = new SparkConf().setAppName("testPrepareData2").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    String fromDatabase = "testdb_in2";
    String toDatabase = "testdb_out2";
    ProducerService producer = new ResignationPrediction();

    producer.prepareData(sc, fromDatabase, toDatabase);

    HiveService hive = new HiveService("localhost", "10000", toDatabase, "khuongav", "");
    hive.openConnection();
    ResultSet rs = hive.executeQuery("select * from input");

    final int num_col = 47;
    String actual2 = "";
    if (rs.next()) {
      for (int i = 1; i <= num_col; i++) {
        if (i == num_col) {
          actual2 = actual2.concat(rs.getString(i) + ",");
        } else {
          actual2 = actual2.concat(rs.getString(i) + ",");
        }
      }
    }

    hive.closeConnection();
    sc.stop();

    assertEquals(expectedPrepareData2, actual2);

  }

  private static boolean deleteDirectory(String path) {
    File directory = new File(path);

    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          if (files[i].isDirectory()) {
            deleteDirectory(files[i].getAbsolutePath());
          } else {
            files[i].delete();
          }
        }
      }
    }
    return (directory.delete());
  }

}
