package bdcore.batch.batchview.hive;

import bdcore.batch.utils.HiveConfiguration;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Hive Service.
 * 
 * @since 7-8-2015
 */
public class HiveService {

  private static final Logger LOG = Logger.getLogger(HiveService.class);
  private String host;

  /**
   * Get Host.
   * 
   * @return hostname
   */
  public String getHost() {
    return host;
  }

  /**
   * Set host name.
   * 
   * @param host : host name
   */
  public void setHost(String host) {
    this.host = host;
  }

  private String port;

  /**
   * Get Hive Port.
   * 
   * @return hive port.
   */
  public String getPort() {
    return port;
  }

  /**
   * Set Hive Port.
   * 
   * @param port : Hive port
   */
  public void setPort(String port) {
    this.port = port;
  }

  private Connection con;

  /**
   * Get Connection.
   * 
   * @return Hive Connection
   */
  public Connection getCon() {
    return con;
  }

  /**
   * Set Connection.
   * 
   * @param con : hive connection
   */
  public void setCon(Connection con) {
    this.con = con;
  }

  private String database;

  /**
   * Get Database.
   * 
   * @return Database name
   */
  public String getDatabase() {
    return database;
  }

  /**
   * Set Database name.
   * 
   * @param database : Database name
   */
  public void setDatabase(String database) {
    this.database = database;
  }

  private String user;

  /**
   * Get User.
   * 
   * @return username
   */
  public String getUser() {
    return user;
  }

  /**
   * Set User.
   * 
   * @param user : user name
   */
  public void setUser(String user) {
    this.user = user;
  }

  private String password;

  /**
   * Get Password.
   * 
   * @return Password
   */
  public String getPassword() {
    return password;
  }

  /**
   * Set Password.
   * 
   * @param password : Hive Password
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Reset Connection.
   * 
   * @throws SQLException : SQL Exception
   */
  public void resetConnection() throws SQLException {
    if (con != null) {
      con.close();
      con = null;
    }
    con =
        DriverManager.getConnection("jdbc:hive2://" + getHost() + ":" + getPort() + "/"
            + getDatabase(), getUser(), getPassword());
  }

  /**
   * Constructor Hive Service.
   * 
   * @param host : hive host
   * @param port : hive port
   * @param database : hive database
   * @param user : username
   * @param password : password
   */
  public HiveService(String host, String port, String database, String user, String password) {
    LOG.info("Set value for host, port, database, user, password");
    LOG.debug("host: " + host + " port: " + port + " database: " + database + "User:" + user
        + " Password: " + password);
    this.setHost(host);
    this.setPort(port);
    this.setDatabase(database);
    this.setUser(user); // default is ""
    this.setPassword(password); // default is ""
    try {
      LOG.info("Set Driver Name");
      String driverName = HiveConfiguration.DRIVER_NAME;
      LOG.debug("Driver Name: " + driverName);
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Open Connection.
   */
  public void openConnection() {
    // Connection: Initialization connection to Hive via host & port.
    LOG.info("Initialization connection");
    LOG.debug("host: " + getHost() + " port: " + getPort() + " database: " + getDatabase()
        + "User:" + getUser() + " Password: " + getPassword());
    try {
      con =
          DriverManager.getConnection("jdbc:hive2://" + getHost() + ":" + getPort() + "/"
              + getDatabase(), getUser(), getPassword());
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    }
  }

  /**
   * Create table based on HiveQL.
   * 
   * @param tableName : Table Name
   * @param columns : List columns in table
   * @param attributes : List attribute with each column in table
   * @return true : success, false : fail
   * @throws SQLException : SQL Exception
   */
  public boolean createTable(String tableName, List<String> columns, List<String> attributes)
      throws SQLException {
    LOG.debug("Number of Columns: " + columns.size());
    LOG.debug("Number of Attributes: " + attributes.size());
    if ((columns.size() != attributes.size()) || columns.size() <= 0) {
      LOG.error("Number of columns or number of attributes error");
      return false;
    }
    // query to create table
    String strQuery = "create table if not exists " + tableName;
    String strCreateCol = toCreateColQl(columns, attributes);
    strQuery = strQuery + " (" + strCreateCol + " )";
    return execute(strQuery);
  }


  /**
   * Create Table from Query. Using AS syntax
   * 
   * @param tableName : Table Name
   * @param query : Query String
   * @return success/fail
   * @throws SQLException : SQL Exception
   */
  public boolean createTable(String tableName, String query) throws SQLException {
    LOG.debug("Table Name: " + tableName);
    LOG.debug("Query: " + query);
    String strQuery = "create table if not exists " + tableName + " AS " + query;
    return execute(strQuery);
  }

  /**
   * Create table read input from CSV based on HiveQL.
   * 
   * @param tableName : Table Name
   * @param columns : List columns in table
   * @param attributes : List attribute with each column in table
   * @return true : success, false : fail
   * @throws SQLException : SQL Exception
   */
  public boolean createTablefromCsv(String tableName, List<String> columns,
      List<String> attributes, String path) throws SQLException {
    LOG.debug("Number of Columns: " + columns.size());
    LOG.debug("Number of Attributes: " + attributes.size());
    if ((columns.size() != attributes.size()) || columns.size() <= 0) {
      LOG.error("Number of columns or number of attributes error");
      return false;
    }
    // query to create table
    String strQuery = "create table if not exists " + tableName;
    String strCreateCol = toCreateColQl(columns, attributes);
    strQuery =
        strQuery + " (" + strCreateCol + " )" + " row format delimited fields terminated by ','";
    execute(strQuery);

    strQuery = "load data inpath '" + path + "' overwrite into table " + tableName;
    return execute(strQuery);
  }

  /**
   * Create Table which is stored as textfile from Query. Using AS syntax
   * 
   * @param tableName : Table Name
   * @param query : Query String
   * @return success/fail
   * @throws SQLException : SQL Exception
   */
  public boolean createCsvTable(String tableName, String query) throws SQLException {
    LOG.debug("Table Name: " + tableName);
    LOG.debug("Query: " + query);
    String strQuery =
        "create table if not exists " + tableName
            + " row format delimited fields terminated by ',' " + " AS " + query;
    return execute(strQuery);
  }

  /**
   * Get Schema Columns of a specific table.
   * 
   * @param tableName an Table Name in database
   * @return A Schema Columns of Table with Name/Type
   * @throws SQLException SQL Exception
   */
  public ResultSet getSchemaColumns(String tableName) throws SQLException {
    String strQuery = "Describe " + tableName;
    return executeQuery(strQuery);
  }

  /**
   * Insert data from a query result to a table.
   * 
   * @param tableName : Table Name
   * @param strQueryData : Query to get data
   * @return {@link Boolean}
   * @throws SQLException : SQL Exception
   */
  public boolean insertTable(String tableName, String strQueryData) throws SQLException {
    String strQuery = "INSERT OVERWRITE TABLE " + tableName + " " + strQueryData;
    LOG.debug("Insert Table Query String: " + strQuery);
    return execute(strQuery);
  }

  /**
   * Delete a specific table.
   * 
   * @param tableName : Table Name
   * @return true/false
   * @throws SQLException : SQL Exception
   */
  public boolean deleteTable(String tableName) throws SQLException {
    String strQuery = "drop table " + tableName;
    return execute(strQuery);
  }

  /**
   * Close Connection.
   */
  public void closeConnection() {
    try {
      this.con.close();
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    }
  }

  /**
   * Execute a query return a boolean.
   * 
   * @param strQuery : Content query
   * @return true/false
   * @throws SQLException : SQL Exception
   */
  public boolean execute(String strQuery) throws SQLException {
    Statement stmt = this.con.createStatement();
    return stmt.execute(strQuery);
  }

  /**
   * Executes the given SQL statement, which returns a single ResultSet object.
   * 
   * @param strQuery : Content query
   * @return ResultSet object that contains the data produced by the given query; never null
   * @throws SQLException : SQL Exception
   */
  public ResultSet executeQuery(String strQuery) throws SQLException {
    Statement stmt = this.con.createStatement();
    return stmt.executeQuery(strQuery);
  }

  /**
   * Create a database.
   * 
   * @param database : Database name
   * @return success or fail
   * @throws SQLException : SQL exception
   */
  public boolean createDatabase(String database) throws SQLException {
    String sql = "CREATE DATABASE IF NOT EXISTS " + database;
    return execute(sql);
  }

  /**
   * Remove a database.
   * 
   * @param database : Database name
   * @return success or fail
   * @throws SQLException : SQL exception
   */
  public boolean removeDatabase(String database) throws SQLException {
    String sql = "DROP DATABASE " + database;
    return execute(sql);
  }

  /**
   * Export data as csv to hdfs path.
   * 
   * @param path : location to export on hdfs
   * @return success or fail
   * @throws SQLException : SQL exception
   */
  public boolean exportCsV(String path, String tableName) throws SQLException {
    String sql =
        "insert overwrite directory '" + path + "' "
            + "row format delimited fields terminated by ',' " + "stored as textfile "
            + "select * from  " + tableName;
    return execute(sql);
  }

  /**
   * Parse to Query string to create Columns.
   * 
   * @param columns : List name of columns
   * @param attributes : List attributes of columns
   * @return Query String
   */
  protected String toCreateColQl(List<String> columns, List<String> attributes) {
    // query to create table
    String strCreateCol = ""; // Init columns & attributes
    for (int i = 0; i < columns.size(); i++) {
      strCreateCol += columns.get(i) + " " + attributes.get(i) + ",";
    }
    // Remove end chracter ","
    strCreateCol = strCreateCol.substring(0, strCreateCol.length() - 1);
    return strCreateCol;
  }
}
