package bdcore.batch.producer.strategy.regression;

import bdcore.batch.batchview.hbase.DataHBase;
import bdcore.batch.batchview.hbase.HBaseService;
import bdcore.batch.producer.strategy.AnalysisStrategy;
import bdcore.batch.utils.HdfsUtil;
import bdcore.infrastructure.common.Constants;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Logsitic Regression.
 * 
 * @since 7-8-2015
 */
public class JavaLogisticRegression implements AnalysisStrategy, Serializable {

  private static final Pattern COMMA = Pattern.compile(",");
  static Logger log = Logger.getLogger(JavaLogisticRegression.class.getName());
  final String user = "khuongav";
  /**
   * Run Logsitic Regression algorithm on the data.
   * 
   * @param input : location of input data
   * @param outputTableName : name of the table contains data
   * @throws IOException of importing data
   */
  public void analyzeData(JavaSparkContext sc, String input, final String outputTableName)
      throws IOException {

    JavaRDD<String> file = sc.textFile(input);

    // Read data from CSV
    JavaPairRDD<String, LabeledPoint> data =
        file.mapToPair(new PairFunction<String, String, LabeledPoint>() {

          public Tuple2<String, LabeledPoint> call(String row) {
            String[] strs = COMMA.split(row);

            String staff = strs[0];

            String[] valueslbP = COMMA.split(row.substring(staff.length() + 1));

            // process NULL value
            for (int i = 0; i < valueslbP.length; i++) {
              if ("\\N".equals(valueslbP[i])) {
                valueslbP[i] = "0";
              }
            }

            double[] doubleValueslbP = new double[valueslbP.length - 1];

            for (int i = 0; i < valueslbP.length - 1; i++) {
              doubleValueslbP[i] = Double.parseDouble(valueslbP[i]);
            }

            LabeledPoint lbP =
                new LabeledPoint(Double.parseDouble(valueslbP[valueslbP.length - 1]), Vectors
                    .dense(doubleValueslbP));

            return new Tuple2<String, LabeledPoint>(staff, lbP);
          }
        });

    // Split initial RDD into two... [70% training data, 40% testing data]

    JavaRDD<LabeledPoint> training = data.values().sample(false, 0.7, 11L);
    training.cache();
    JavaRDD<LabeledPoint> test = data.values().subtract(training);

    int numIterations = 1000;
    double threshlod = 0.01;

    // Load model
    // LogisticRegressionModel sameModel =
    // LogisticRegressionModel.load(sc.sc(), "/MLlib/LogisticRegression/model");

    final LogisticRegressionModel model =
        LogisticRegressionWithSGD.train(training.rdd(), numIterations, threshlod);

    // Save model
    // the directory is processed to make sure it is available
    String modelPath = "/" + user + "/LogisticRegression/model";
    
    if (!sc.isLocal()) {
      HdfsUtil.removeExistDir(modelPath);
    }
    model.save(sc.sc(), modelPath);

    testAccuracy(test, model);

    importData(outputTableName, sc, data, model);
  }

  private void testAccuracy(JavaRDD<LabeledPoint> test, final LogisticRegressionModel model) {
    JavaPairRDD<Double, Double> predictionAndLabel =
        test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
          @Override
          public Tuple2<Double, Double> call(LabeledPoint lbP) {
            return new Tuple2<Double, Double>(lbP.label(), model.predict(lbP.features()));
          }
        });

    long count = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
      @Override
      public Boolean call(Tuple2<Double, Double> pl) {
        if (pl._1().compareTo(pl._2()) == 0) {
          return true;
        }
        return false;
      }
    }).count();

    final double accuracy = count / test.count();

    log.info("Precision: " + accuracy);

    long trueNegative = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
      @Override
      public Boolean call(Tuple2<Double, Double> pl) {
        if (pl._1() == 0 && pl._2() == 0) {
          return true;
        }
        return false;
      }
    }).count();

    long falseNegative = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
      @Override
      public Boolean call(Tuple2<Double, Double> pl) {
        if (pl._1() == 0 && pl._2() == 1) {
          return true;
        }
        return false;
      }
    }).count();

    long falsePositive = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
      @Override
      public Boolean call(Tuple2<Double, Double> pl) {
        if (pl._1() == 1 && pl._2() == 0) {
          return true;
        }
        return false;
      }
    }).count();

    long truePositive = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
      @Override
      public Boolean call(Tuple2<Double, Double> pl) {
        if (pl._1() == 1 && pl._2() == 1) {
          return true;
        }
        return false;
      }
    }).count();

    double sensitivity = truePositive / (truePositive + falseNegative);

    double specificity = trueNegative / (trueNegative + falsePositive);

    log.info("trueNegative: " + trueNegative);
    log.info("falseNegative: " + falseNegative);
    log.info("falsePositive: " + falsePositive);
    log.info("truePositive: " + truePositive);

    log.info("Sensitivity: " + sensitivity);
    log.info("Specificity: " + specificity);

  }

  private void importData(final String outputTableName, JavaSparkContext sc,
      JavaPairRDD<String, LabeledPoint> data, final LogisticRegressionModel model)
      throws IOException {

    // Save data to HBase

    final HBaseService hbase = new HBaseService("hbase:" + Constants.HBASE_MASTER_PORT);
    hbase.openConnection();
    String[] colF = {"content"};
    hbase.createTable(outputTableName, colF);
    hbase.closeConnection();

    final Broadcast<String[]> broadcastVar =
        sc.broadcast(new String[] {"avg_timespent", "diff_time", "times_sat_sun",
            "times_ten", "num_daysoff", "urgent", "high", "normal", "low", "verylow", "onhold",
            "issuetype1", "issuetype3", "issuetype5", "issuetype9", "issuetype10", "issuetype11",
            "issuetype12", "issuetype13", "issuetype14", "issuetype15", "issuetype16",
            "issuetype17", "issuetype18", "issuetype19", "issuetype20", "issuetype21",
            "issuetype22", "issuetype23", "issuetype24", "issuetype25", "issuetype26",
            "issuetype27", "issuetype28", "issuetype29", "issuetype30", "issuetype31",
            "issuetype32", "issuetype33", "issuetype34", "issuetype35", "issuetype36",
            "issuetype37", "issuetype38", "issuetype39", "is_off", "predict"});

    // Because the class HBaseService is not serializable 
    // so it must be declared in side lamba fuctiom
    // To avoid outofmemory exception, make RDD into one partition
    data.repartition(1).foreach(new VoidFunction<Tuple2<String, LabeledPoint>>() {
      @Override
      public void call(Tuple2<String, LabeledPoint> t2) throws IOException {

        String[] valueNamesWithPredict = broadcastVar.value();
        String staff = t2._1;
        String attributes = t2._2.features().toString().replace("[", "").replace("]", "");
        double predictValue = model.predict(t2._2.features());

        String values =
            attributes + "," + Double.toString(predictValue);
        String[] valuesWithPredict = COMMA.split(values);

        HBaseService hbase = new HBaseService("hbase:" + Constants.HBASE_MASTER_PORT);
        hbase.openConnection();

        for (int i = 0; i < valuesWithPredict.length; i++) {

          List<byte[]> colName = new ArrayList<byte[]>();
          colName.add(Bytes.toBytes(valueNamesWithPredict[i]));
          List<byte[]> colData = new ArrayList<byte[]>();
          colData.add(Bytes.toBytes(valuesWithPredict[i]));

          DataHBase data = new DataHBase(Bytes.toBytes(staff), colName, colData);
          hbase.addData(outputTableName, "content", new DataHBase[] {data});
        }
        hbase.closeConnection();

      }
    });

  }
}
