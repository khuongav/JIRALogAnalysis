package bdcore.batch.batchview.hbase;

import bdcore.infrastructure.common.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Hbase Service.
 * 
 * @since 7-8-2015
 */
public class HBaseService {

  private static Logger LOG = Logger.getLogger(HBaseService.class.getName());
  private Admin hba;
  private Connection connection;
  private Configuration conf;

  /**
   * Constructor HBaseServiceImp with hostname (as hbase).
   * 
   * @param hostname HBase hostname
   * @throws IOException I/O exception
   */
  public HBaseService(String hostname) throws IOException {
    // Config Hbase base on Constant
    conf = HBaseConfiguration.create();
    // Default : hbase.zookeeper.quorum
    conf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hostname);
    // Default : hbase.zookeeper.property.clientPort : 2181
    conf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT,
        Constants.HBASE_ZOOKEEPER_QUORUM_PORT);
  }

  /**
   * Open connection of service.
   */
  public void openConnection() {
    try {
      LOG.info("Open connection");
      connection = ConnectionFactory.createConnection(conf);
      hba = connection.getAdmin();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  /**
   * Close connection of service.
   */
  public void closeConnection() {
    try {
      LOG.info("Close connection");
      connection.close();
      hba.close();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  /**
   * Initialization a table with a list column Family.
   * 
   * @param strTableName Table Name
   * @param colF list column family
   * @return true/false
   * @throws IOException I/O Exception
   */
  public boolean createTable(String strTableName, String[] colF) throws IOException {
    // If Table not exists
    TableName tableName = TableName.valueOf(strTableName);
    if (!hba.tableExists(tableName)) {
      LOG.debug("Create a Table: " + strTableName);
      HTableDescriptor ht = new HTableDescriptor(tableName);
      try {
        // Create columns family
        for (int i = 0; i < colF.length; i++) {
          LOG.debug("create column family " + colF[i]);
          HColumnDescriptor columnF = new HColumnDescriptor(colF[i]);
          ht.addFamily(columnF);
        }
        hba.createTable(ht);
        // hba.disableTable(tableName);
      } catch (IOException e) {

        LOG.error(e.toString());
        closeConnection();
        System.exit(1);
      }
      LOG.info("Done!");
      return true;
    }
    return false;
  }

  /**
   * Add data to a column family of a table in HBase.
   * 
   * @param strTableName Table Name
   * @param colF Column Family name
   * @param data Data Hbase
   * @throws IOException I/O Exception
   */
  public void addData(String strTableName, String colF, DataHBase[] data) throws IOException {
    TableName tableName = TableName.valueOf(strTableName);
    Table table = connection.getTable(tableName);
    for (int i = 0; i < data.length; i++) {
      LOG.debug("Row Key " + i + ": " + data[i].getRowKey());
      Put put = new Put(data[i].getRowKey());
      // put data each column into row key
      for (int icol = 0; icol < data[i].getColName().size(); icol++) {
        put.addColumn(Bytes.toBytes(colF), data[i].getColName().get(icol), data[i].getColData()
            .get(icol));
      }
      table.put(put);
    }
    table.close();
  }

  /**
   * Add relation to a other table by name of column in a column family.
   * 
   * @param sourceTable source table
   * @param destTable destination table
   * @param rowKey row key on source table
   * @param relationRow rowKey on destination table
   * @throws IOException I/O Exception
   */
  public void addRelationData(TableName sourceTable, String destTable, String rowKey,
      String[] relationRow) throws IOException {
    Table table = connection.getTable(sourceTable);
    LOG.debug("Row Key: " + rowKey);
    Put put = new Put(Bytes.toBytes(rowKey));
    // put data each column into row key
    for (int irelationRow = 0; irelationRow < relationRow.length; irelationRow++) {
      put.addColumn(Bytes.toBytes(destTable), Bytes.toBytes(relationRow[irelationRow]), null);
    }
    table.put(put);
    table.close();
  }

  /**
   * Get row key on a table.
   * 
   * @param tableName the table name on database
   * @param rowKey the row key of data on database
   * @return {@link Result}
   * @throws IOException {@link IOException}
   */
  public Result get(String tableName, String rowKey) throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    Result result = table.get(get);
    return result;
  }

  /**
   * Get row key on a table.
   * 
   * @param tableName the table name on database
   * @param rowKey the row key of data on database
   * @param colF the column family of data on database
   * @return {@link Result}
   * @throws IOException {@link IOException}
   */
  public Result get(String tableName, String rowKey, String colF) throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addFamily(Bytes.toBytes(colF));
    Result result = table.get(get);
    return result;
  }

  /**
   * Get row key on a table.
   * 
   * @param tableName the table name on database
   * @param rowKey the row key of data on database
   * @param colF the column family of data on database
   * @param maxVersions the number of versions of data on database
   * @return {@link Result}
   * @throws IOException {@link IOException}
   */
  public Result get(String tableName, String rowKey, String colF, int maxVersions)
      throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addFamily(Bytes.toBytes(colF));
    get.setMaxVersions(maxVersions);
    Result result = table.get(get);
    return result;
  }

  /**
   * Get row key on a table.
   * 
   * @param tableName the table name on database
   * @param rowKey the row key of data on database
   * @param maxVersions the number of versions of data on database
   * @return {@link Result}
   * @throws IOException {@link IOException}
   */
  public Result get(String tableName, String rowKey, int maxVersions) throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    get.setMaxVersions(maxVersions);
    Result result = table.get(get);
    return result;
  }
}
