package bdcore.batch.producer.prediction;

import bdcore.batch.batchview.hive.HiveService;
import bdcore.batch.producer.ProducerService;
import bdcore.batch.producer.strategy.AnalysisStrategy;
import bdcore.batch.utils.SparkUtil;
import bdcore.infrastructure.common.Constants;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Resignation prediction.
 * 
 * @since 7-8-2015
 */
public class ResignationPrediction implements ProducerService {

  final String stringType = "string";
  final String floatType = "float";
  final String intType = "int";
  final String bigintType = "bigint";
  
  final String user = "khuongav";


  /**
   * Prepare the necessary data for analysis.
   * 
   * @param sc : Spark context
   * @param fromDatabase : database to get data
   * @param toDatabase : database to contain processed data
   * @throws SQLException of queries
   * @throws IOException of removing existing directory
   */
  public void prepareData(JavaSparkContext sc, String fromDatabase, String toDatabase)
      throws SQLException, IOException {

    HiveService hive = new HiveService("hive", Constants.HIVE_MASTER_PORT, fromDatabase, user, "");
    hive.openConnection();
    hive.createDatabase(toDatabase);
    String tableName;
    String sql;

    final int max_year_of_build_data = 2015;
    tableName = toDatabase + ".jiraissue_build";
    sql = "select * from jiraissue where year(created) < " + max_year_of_build_data;
    hive.createTable(tableName, sql);

    tableName = toDatabase + ".worklog_build";
    sql = "select * from worklog where year(created) < " + max_year_of_build_data;
    hive.createTable(tableName, sql);

    tableName = toDatabase + ".jiraissue_evaluation";
    sql = "select * from jiraissue";
    hive.createTable(tableName, sql);

    tableName = toDatabase + ".worklog_evaluation ";
    sql = "select * from worklog";
    hive.createTable(tableName, sql);

    tableName = toDatabase + ".priority ";
    sql = "select * from priority";
    hive.createTable(tableName, sql);

    hive.setDatabase(toDatabase);
    hive.resetConnection();

    // the date on Jira is one day slower
    final int saturday = 5;
    final int sunday = 6;
    final int ten_hours_to_seconds = 36000;
    final int num_dayswork_week = 5;
    final int num_daysoff_considered_resigned = 25;

    final String jiraissue = "jiraissue_evaluation";
    final String worklog = "worklog_evaluation";
    final String jiramaxdate = "2015-04-25";

    // get the percentages of issuetypes of staffs
    preparePercentageIssuetypeData(hive, jiraissue);

    // get the percentages of priorities of staffs
    preparePercentagePriorityData(hive, jiraissue);

    // calculate the average timespent
    // and the total difference of timespent vs original estimated time
    prepareAvgTimespentDiffTimeData(hive, jiraissue);

    // number of times log work more than 10 hours
    prepareGtTenHousWorkLogData(hive, saturday, sunday, ten_hours_to_seconds, worklog);

    // number of times log work on Saturday and Sunday
    prepareSatSunLogWorkData(hive, saturday, sunday, worklog);

    // number of daysoff
    prepareDaysOffData(hive, saturday, sunday, num_dayswork_week, worklog);

    // check if the staff already resigned form job
    prepareResignationData(hive, num_daysoff_considered_resigned, worklog, jiramaxdate);

    // join all the tables into one input
    joinData2Input(sc, toDatabase, hive);

    hive.closeConnection();

  }

  private void joinData2Input(JavaSparkContext sc, String toDatabase, HiveService hive)
      throws SQLException, IOException {
    String tableName;
    String sql;

    tableName = "hr";

    sql =
        "select re.staff, da.num_daysoff, re.is_off "
            + "from days_off da join resignation re on da.staff = re.staff";

    hive.createTable(tableName, sql);

    tableName = "temp";

    sql =
        "select coalesce(t3.staff, tmp.staff) as staff, t3.avg_timespent, t3.diff_time, "
            + "tmp.times_ten, tmp.times_sat_sun from ( "
            + "select coalesce(t2.staff, t1.staff) as staff, t2.times_ten, t1.times_sat_sun "
            + "from sat_sun_logwork t1 full outer join gt_tenhours_logwork t2 "
            + "on (t1.staff = t2.staff) "
            + ") tmp full outer join avg_timespent_diff_time t3 on (tmp.staff = t3.staff)";

    hive.createTable(tableName, sql);

    tableName = "temp1";

    sql =
        "select temp.staff, temp.avg_timespent, temp.diff_time, temp.times_ten, "
            + "temp.times_sat_sun, hr.num_daysoff, hr.is_off "
            + "from temp join hr on temp.staff = hr.staff";

    hive.createTable(tableName, sql);

    tableName = "percentage_priority_issuetype";

    sql =
        "select per_pri.staff, per_pri.priority, per_pri.percentage as percentage_priority, "
            + "per_iss.issuetype, per_iss.percentage as percentage_issuetype "
            + "from percentage_priority per_pri join percentage_issuetype per_iss on "
            + "per_pri.staff = per_iss.staff";

    hive.createCsvTable(tableName, sql);

    String inputSpark = "/user/hive/warehouse/" + toDatabase + ".db/percentage_priority_issuetype";

    // hive.exportCSV(inputSpark,tableName);

    // Parse percentage columns
    // the directory will be processed to make sure it is available
    String outputSpark = "/" + user + "/outputSpark";

    SparkUtil.parsePercentageColumn(sc, inputSpark, outputSpark);

    tableName = "temp2";

    List<String> columns =
        new ArrayList<>(Arrays.asList("staff", "urgent", "high", "normal", "low", "verylow",
            "onhold", "issuetype1", "issuetype3", "issuetype5", "issuetype9", "issuetype10",
            "issuetype11", "issuetype12", "issuetype13", "issuetype14", "issuetype15",
            "issuetype16", "issuetype17", "issuetype18", "issuetype19", "issuetype20",
            "issuetype21", "issuetype22", "issuetype23", "issuetype24", "issuetype25",
            "issuetype26", "issuetype27", "issuetype28", "issuetype29", "issuetype30",
            "issuetype31", "issuetype32", "issuetype33", "issuetype34", "issuetype35",
            "issuetype36", "issuetype37", "issuetype38", "issuetype39"));

    List<String> attributes =
        new ArrayList<>(Arrays.asList(stringType, floatType, floatType, floatType, floatType,
            floatType, floatType, floatType, floatType, floatType, floatType, floatType, floatType,
            floatType, floatType, floatType, floatType, floatType, floatType, floatType, floatType,
            floatType, floatType, floatType, floatType, floatType, floatType, floatType, floatType,
            floatType, floatType, floatType, floatType, floatType, floatType, floatType, floatType,
            floatType, floatType, floatType, floatType));

    hive.createTablefromCsv(tableName, columns, attributes, outputSpark);

    tableName = "input";

    sql =
        "select t1.staff, t1.avg_timespent, t1.diff_time, t1.times_sat_sun, t1.times_ten, "
            + "t1.num_daysoff, t2.urgent, t2.high, t2.normal, t2.low, t2.verylow, t2.onhold, "
            + "t2.issuetype1, t2.issuetype3, t2.issuetype5, t2.issuetype9, t2.issuetype10, "
            + "t2.issuetype11, t2.issuetype12, "
            + "t2.issuetype13, t2.issuetype14, t2.issuetype15, t2.issuetype16, t2.issuetype17, "
            + "t2.issuetype18, t2.issuetype19, "
            + "t2.issuetype20, t2.issuetype21, t2.issuetype22, t2.issuetype23, t2.issuetype24, "
            + "t2.issuetype25, t2.issuetype26, "
            + "t2.issuetype27, t2.issuetype28, t2.issuetype29, t2.issuetype30, t2.issuetype31, "
            + "t2.issuetype32, t2.issuetype33, "
            + "t2.issuetype34, t2.issuetype35, t2.issuetype36, t2.issuetype37, t2.issuetype38, "
            + "t2.issuetype39, t1.is_off from temp2 t2 join temp1 t1 on t2.staff = t1.staff";

    hive.createCsvTable(tableName, sql);

    // hive.exportCSV(output,tableName);
  }

  private void prepareResignationData(HiveService hive, final int daysoffConsideredResigned,
      final String worklog, final String jiramaxdate) throws SQLException {

    // 1 means off
    // 0 means still working
    String tableName = "resignation";
    List<String> columns = new ArrayList<>(Arrays.asList("staff", "is_off"));
    List<String> attributes = new ArrayList<>(Arrays.asList(stringType, intType));

    hive.createTable(tableName, columns, attributes);

    String sql =
        "select tmp.author, case when datediff('" + jiramaxdate + "',tmp.max_date) > "
            + daysoffConsideredResigned + " then 1 else 0 end from ( "
            + "select author, max(to_date(startdate)) as max_date from " + worklog + " "
            + "group by author ) tmp";

    hive.insertTable(tableName, sql);
  }

  private void prepareDaysOffData(HiveService hive, final int saturday, final int sunday,
      final int daysworkWeek, final String worklog) throws SQLException {

    String tableName;
    List<String> columns;
    List<String> attributes;

    tableName = "days_work";
    columns = new ArrayList<>(Arrays.asList("staff", "year", "week", "num_dayswork"));
    attributes = new ArrayList<>(Arrays.asList(stringType, intType, intType, intType));

    hive.createTable(tableName, columns, attributes);

    String sql;
    // because the date on Jira is one day slower so it need to be added one date
    sql =
        "select author, year(date_add(startdate,1)), weekofyear(date_add(startdate,1)), "
            + "count(distinct day(date_add(startdate,1))) from " + worklog + " "
            + "where from_unixtime(unix_timestamp(startdate),'u') != " + saturday + " and "
            + "from_unixtime(unix_timestamp(startdate),'u') != " + sunday + " "
            + "group by author, year(date_add(startdate,1)), weekofyear(date_add(startdate,1))";

    hive.insertTable(tableName, sql);

    tableName = "min_max_startdate";
    columns = new ArrayList<>(Arrays.asList("staff", "min_date", "max_date"));
    attributes = new ArrayList<>(Arrays.asList(stringType, stringType, stringType));

    hive.createTable(tableName, columns, attributes);

    sql =
        "select author, min(date_add(startdate,1)), max(date_add(startdate,1)) from " + worklog
            + " group by author";

    hive.insertTable(tableName, sql);

    tableName = "days_off";
    columns = new ArrayList<>(Arrays.asList("staff", "num_daysoff"));
    attributes = new ArrayList<>(Arrays.asList(stringType, intType));

    hive.createTable(tableName, columns, attributes);

    sql =
        "select tmp.staff, sum(tmp.num_daysoff) from ( select dw.staff, dw.year, dw.week, ("
            + daysworkWeek + " - dw.num_dayswork) as num_daysoff "
            + "from days_work dw join min_max_startdate sta on dw.staff = sta.staff "
            + "where (dw.year != year(sta.min_date) or dw.week != weekofyear(sta.min_date)) "
            + "and (dw.year != year(sta.max_date) or dw.week != weekofyear(sta.max_date)) "
            + ") tmp group by tmp.staff";

    hive.insertTable(tableName, sql);
  }

  private void prepareSatSunLogWorkData(HiveService hive, final int saturday, final int sunday,
      final String worklog) throws SQLException {

    String tableName = "sat_sun_logwork";
    List<String> columns = new ArrayList<>(Arrays.asList("staff", "times_sat_sun"));
    List<String> attributes = new ArrayList<>(Arrays.asList(stringType, intType));

    hive.createTable(tableName, columns, attributes);

    String sql =
        "select author, count(distinct to_date(startdate)) from " + worklog + " "
            + "where from_unixtime(unix_timestamp(startdate),'u') = " + saturday + " or "
            + "from_unixtime(unix_timestamp(startdate),'u') = " + sunday + " group by author";

    hive.insertTable(tableName, sql);
  }

  private void prepareGtTenHousWorkLogData(HiveService hive, final int saturday, final int sunday,
      final int seconds, final String worklog) throws SQLException {

    String tableName = "gt_tenhours_logwork";
    List<String> columns = new ArrayList<>(Arrays.asList("staff", "times_ten"));
    List<String> attributes = new ArrayList<>(Arrays.asList(stringType, intType));

    hive.createTable(tableName, columns, attributes);

    String sql =
        "select tmp.author, count(*) from ( "
            + "select wo.author, to_date(wo.startdate), sum(wo.timeworked) from " + worklog
            + " wo where from_unixtime(unix_timestamp(startdate),'u') != " + saturday
            + " and from_unixtime(unix_timestamp(startdate),'u') != " + sunday + " "
            + "group by wo.author, to_date(wo.startdate) having sum(wo.timeworked) > " + seconds
            + " ) tmp group by tmp.author";

    hive.insertTable(tableName, sql);
  }

  private void prepareAvgTimespentDiffTimeData(HiveService hive, final String jiraissue)
      throws SQLException {

    String tableName = "avg_timespent_diff_time";
    List<String> columns = new ArrayList<>(Arrays.asList("staff", "avg_timespent", "diff_time"));
    List<String> attributes = new ArrayList<>(Arrays.asList(stringType, bigintType, bigintType));

    hive.createTable(tableName, columns, attributes);

    String sql =
        "select tmp.assignee, tmp.sum_timespent/num, abs(tmp.sum_timespent - tmp.sum_original) "
            + "from ( "
            + "select tmp_cleaned.assignee, sum(tmp_cleaned.timespent_cleaned) sum_timespent, "
            + "sum(tmp_cleaned.timeoriginalestimate_cleaned) sum_original, count(*) num "
            + "from ( select ji.assignee, "
            + "case when ji.timespent is null then 0 when ji.timeoriginalestimate is null then 0 "
            + "when abs(ji.timespent - ji.timeoriginalestimate) > 5*timespent then 0 "
            + "else ji.timespent end as timespent_cleaned, "
            + "case when ji.timespent is null then 0 when ji.timeoriginalestimate is null then 0 "
            + "when abs(ji.timespent - ji.timeoriginalestimate) > 5*timespent then 0 "
            + "else ji.timeoriginalestimate end as timeoriginalestimate_cleaned from " + jiraissue
            + " ji) tmp_cleaned group by tmp_cleaned.assignee ) tmp";

    hive.insertTable(tableName, sql);
  }

  private void preparePercentagePriorityData(HiveService hive, final String jiraissue)
      throws SQLException {

    String tableName = "percentage_priority";
    List<String> columns = new ArrayList<>(Arrays.asList("staff", "priority", "percentage"));
    List<String> attributes = new ArrayList<>(Arrays.asList(stringType, stringType, floatType));

    hive.createTable(tableName, columns, attributes);

    String sql =
        "select tmp.assignee, tmp.pname, tmp.num/sum(tmp.num) over (partition by tmp.assignee) "
            + "from ( select ji.assignee, pri.pname, count(*) as num from " + jiraissue
            + " ji join priority pri on ji.priority = pri.id group by ji.assignee, pri.pname "
            + ") tmp";

    hive.insertTable(tableName, sql);
  }

  private void preparePercentageIssuetypeData(HiveService hive, final String jiraissue)
      throws SQLException {

    String tableName = "percentage_issuetype";
    List<String> columns = new ArrayList<>(Arrays.asList("staff", "issuetype", "percentage"));
    List<String> attributes = new ArrayList<>(Arrays.asList(stringType, intType, floatType));

    hive.createTable(tableName, columns, attributes);

    String sql =
        "select tmp.assignee, tmp.issuetype, tmp.num/sum(tmp.num) over (partition by tmp.assignee) "
            + "from ( select ji.assignee, ji.issuetype, count(*) as num from " + jiraissue
            + " ji group by ji.assignee, ji.issuetype ) tmp";

    hive.insertTable(tableName, sql);
  }

  /**
   * Run machine learning algorithm on the data.
   * 
   * @param sc : Spark context
   * @param strategy : algorithm for analyze data
   * @param database : location of input data
   * @throws IOException of importing data
   */
  public void analyzeData(JavaSparkContext sc, AnalysisStrategy strategy, String database,
      String outputTableName) throws IOException {

    String inputStrategy = "/user/hive/warehouse/" + database + ".db/input";
    strategy.analyzeData(sc, inputStrategy, outputTableName);

  }
}
