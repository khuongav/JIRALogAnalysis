package bdcore.batch.producer;

import bdcore.batch.producer.strategy.AnalysisStrategy;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Producer Service.
 * 
 * @since 7-8-2015
 */
public interface ProducerService {

  /**
   * Prepare the necessary data for analysis.
   * 
   * @param sc : Spark context
   * @param fromDatabase : database to get data
   * @param toDatabase : database to contain processed data
   * @param output : location to export data
   * @throws SQLException of queries
   * @throws IOException of removing existing directory
   */
  void prepareData(JavaSparkContext sc, String fromDatabase, String toDatabase) 
      throws SQLException, IOException;

  /**
   * Run machine learning algorithm on the data.
   * 
   * @param sc : Spark context
   * @param strategy : algorithm for analyze data
   * @param database : location of input data
   * @throws IOException of importing data
   */
  void analyzeData(JavaSparkContext sc, AnalysisStrategy strategy, String database,
      String outputTableName) throws IOException;

}
