package bdcore.batch.batchview.hbase;

import java.util.List;

/**
 * Data structure to store to HBase.
 * 
 * @since 7-8-2015
 */
public class DataHBase {
  private byte[] rowKey;
  private List<byte[]> colName;
  private List<byte[]> colData;

  /**
   * Constructor DataHBase.
   * 
   * @param rowKey
   *          Key name
   * @param colData
   *          Key data
   */
  public DataHBase(byte[] rowKey, List<byte[]>  colName, List<byte[]>  colData) {
    this.setRowKey(rowKey);
    this.setColData(colData);
    this.setColName(colName);
  }

  /**
   * Get Row Key.
   * 
   * @return row key byte[]
   */
  public byte[] getRowKey() {
    return rowKey;
  }

  /**
   * Set Row Key.
   * 
   * @param rowKey
   *          Key name
   */
  public void setRowKey(byte[] rowKey) {
    this.rowKey = rowKey;
  }

  /**
   * Get Key Data.
   * 
   * @return Key Data
   */
  public List<byte[]> getColData() {
    return colData;
  }

  /**
   * Set Data key.
   * 
   * @param colData
   *          data key
   */
  public void setColData(List<byte[]> colData) {
    this.colData = colData;
  }
  /**
   * Get Key Data.
   * 
   * @return Key Data
   */
  public List<byte[]> getColName() {
    return colName;
  }

  /**
   * Set Data key.
   * 
   * @param colData
   *          data key
   */
  public void setColName(List<byte[]> colName) {
    this.colName = colName;
  }
}
