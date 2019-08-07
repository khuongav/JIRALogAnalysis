package bdcore.batch.utils;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * Hbase Util.
 * 
 * @since 23-4-2015
 */

public final class HBaseUtil {
  private static Logger log = Logger.getLogger(HBaseUtil.class.getName());

  /**
   * Check a Column Family.
   * 
   * @param hTable : Table in Hbase
   * @param columnFamily : column family in Hbase
   * @return : have/no have column family in Hbase
   * @throws IOException : I/O Exception
   */
  public static boolean existsColumnFamily(HTable hTable, String columnFamily) throws IOException {
    // Get table descriptor
    HTableDescriptor descriptor = hTable.getTableDescriptor();
    // Search columnFamily
    for (HColumnDescriptor column : descriptor.getFamilies()) {
      String columnName = Bytes.toString(column.getName());
      // Found columnFamily
      if (columnName.equals(columnFamily)) {
        return true;
      }
    }
    return false;
  }
}
