package bdcore.batch.utils;

/**
 * Hive Configuration.
 * 
 * @since 23-4-2015
 */
public final class HiveConfiguration {
  public static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  public static final String ES_STORAGE_CLASS = "'org.elasticsearch.hadoop.hive.EsStorageHandler'";
  public static final String ES_RESOURCE_TERM = "'es.resource'";
  public static final String ES_INDEX_AUTO_CREATE_TERM = "'es.index.auto.create'";
  public static final String ES_HOST_TERM = "'es.nodes'";
  public static final String ES_PORT_TERM = "'es.port'";
  public static final String ES_RESOURCE_WRITE_TERM = "'es.resource.write'";
}
