package bdcore.batch.producer.strategy;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Analysis strategy.
 * 
 * @since 7-8-2015
 */
public interface AnalysisStrategy {

  /**
   * Run machine learning algorithm on the data.
   * 
   * @param input : location of input data
   * @param outputTableName : name of the table contains data
   * @throws IOException of importing data
   */
  void analyzeData(JavaSparkContext sc, String input, String outputTableName) throws IOException;

}
