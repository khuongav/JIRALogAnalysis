package bdcore.batch.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Spark utilities.
 * 
 * @since 7-8-2015
 */
public final class SparkUtil {

  private static final Pattern COMMA = Pattern.compile(",");

  /**
   * Parse multiple rows of a staff into one row with full information of priorities and issuetypes.
   * 
   * @param input : location of input data
   * @param output : location of output data
   * @throws IOException of removing existing directory
   */
  public static void parsePercentageColumn(JavaSparkContext sc, String input, String output)
      throws IOException {

    JavaPairRDD<String, Tuple2<String, String>> collection =
        sc.textFile(input).flatMapToPair(
            new PairFlatMapFunction<String, String, Tuple2<String, String>>() {
              public Iterable<Tuple2<String, Tuple2<String, String>>> call(String row) {

                String[] strs = COMMA.split(row);
                List<Tuple2<String, Tuple2<String, String>>> results =
                    new ArrayList<Tuple2<String, Tuple2<String, String>>>();
                for (int i = 1; i <= 3; i = i + 2) {
                  results.add(new Tuple2<String, Tuple2<String, String>>(strs[0],
                      new Tuple2<String, String>(strs[i], strs[i + 1])));
                }
                return results;

              }
            });

    JavaPairRDD<String, Iterable<Tuple2<String, String>>> uniqueRows =
        collection.distinct().groupByKey();

    JavaPairRDD<String, Iterable<String>> processedRows =
        uniqueRows.mapValues(new Function<Iterable<Tuple2<String, String>>, Iterable<String>>() {

          @Override
          public Iterable<String> call(Iterable<Tuple2<String, String>> it2) throws Exception {

            List<String> results = new ArrayList<String>();
            boolean available;
            String[] priorities = {"Urgent", "High", "Normal", "Low", "Very low", "On hold"};
            for (String priority : priorities) {
              available = false;
              for (Tuple2<String, String> tu : it2) {
                if (tu._1.equals(priority)) {
                  results.add(tu._2);
                  available = true;
                  break;
                }
              }
              if (!available) {
                results.add("0");
              }
            }

            //issue type numbers on jira logs
            for (int issuetype = 1; issuetype <= 39; issuetype++) {
              if (issuetype == 2) {
                issuetype = 3;
              } else if (issuetype == 4) {
                issuetype = 5;
              } else if (issuetype == 6) {
                issuetype = 9;
              }
              available = false;
              for (Tuple2<String, String> tu : it2) {
                if (tu._1.equals(Integer.toString(issuetype))) {
                  results.add(tu._2);
                  available = true;
                  break;
                }
              }
              if (!available) {
                results.add("0");
              }
            }
            return results;
          }
        });

    exportCsv(output, sc, processedRows);
  }

  private static void exportCsv(String output, JavaSparkContext sc,
      JavaPairRDD<String, Iterable<String>> processedRows) throws IOException {
    JavaRDD<String> csv =
        processedRows.map(new Function<Tuple2<String, Iterable<String>>, String>() {

          public String call(Tuple2<String, Iterable<String>> ts) throws Exception {
            String strResult = "";
            strResult = strResult.concat(ts._1);

            for (String t : ts._2) {
              strResult = strResult.concat("," + t);
            }
            return strResult;
          }
        });

    if (!sc.isLocal()) {
      HdfsUtil.removeExistDir(output);
    }
    csv.saveAsTextFile(output);
  }
}
