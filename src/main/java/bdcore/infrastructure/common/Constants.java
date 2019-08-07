package bdcore.infrastructure.common;

public class Constants {
  // *************************** Constants HBASE ***********************************//
  public static final String HBASE_FIELDS_PROPERTY_TERMINATED = "\t";
  public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT =
      "hbase.zookeeper.property.clientPort";
  public static final String HBASE_ZOOKEEPER_QUORUM_PORT = "2181";
  public static final String HBASE_HOSTNAME = "hbase";
  public static final String HBASE_MASTER_PORT = "60000";
  public static final String HBASE_TABLE_SESSIONKEY = "SessionKey";
  public static final String HBASE_TABLE_NO = "No";
  
  // *************************** Constants HIVE ***********************************//
  public static final String HIVE_MASTER_PORT = "10000";

}
