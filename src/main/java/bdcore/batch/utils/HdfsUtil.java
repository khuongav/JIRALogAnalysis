package bdcore.batch.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * HDFS utilities.
 * 
 * @since 7-8-2015
 */
public final class HdfsUtil {

  /**
   * Remove directory on hdfs if exist.
   * 
   * @param hdfsPath : location of output data
   * @throws IOException of removing existing directory
   */
  public static void removeExistDir(String hdfsPath) throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("src/main/resources/core-site.xml"));
    FileSystem fs = FileSystem.get(conf);

    if (fs.exists(new Path(hdfsPath))) {
      fs.delete(new Path(hdfsPath), true);
    }
  }
}
