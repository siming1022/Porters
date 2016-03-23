package com.teamsun.porters.move.template;

/**
 * Sqoop命令模板类
 * 
 * @author Administrator
 *
 */
public class SqoopCommandTemplate 
{
	public static final String SQOOP_HDFS_2_ORACLE = "sqoop export --connect \"{0}\" --username {1} --password {2} --table {3} --export-dir {4} --fields-terminated-by {5} --input-null-string {6} --null-non-string {7} --m {8}";
	
	public static final String SQOOP_HDFS_2_TERADATA = "sqoop export --connect \"{0}\" --driver {1} --username {2} --password {3} --table {4} --export-dir {5} --fields-terminated-by {6} --m {7}";

	public static final String SQOOP_HDFS_2_VERTICA = "sqoop export --connect \"{0}\" --username {1} --password {2} --table {3} --export-dir {4} --fields-terminated-by {5} --input-null-string {6} --null-non-string {7} --m {8}";

	public static final String SQOOP_HDFS_2_MYSQL = "sqoop export --connect \"{0}\" --username {1} --password {2} --table {3} --export-dir {4} --fields-terminated-by {5} --input-null-string {6} --null-non-string {7} --m {8}";

	public static final String SQOOP_ORACLE_2_HDFS_BY_SQL = "sqoop import --connect \"{0}\" --username {1} --password {2} --query \"{3}\" --target-dir {4} --input-null-string {5} --null-non-string {6} --fields-terminated-by {7} --append --m {8}";

	public static final String SQOOP_ORACLE_2_HDFS = "sqoop import --connect \"{0}\" --username {1} --password {2} --table {3} --target-dir {4} --input-null-string {5} --null-non-string {6} --fields-terminated-by {7} --append --m {8}";

	public static final String SQOOP_ORACLE_2_HBASE_BY_SQL = "sqoop import --connect \"{0}\" --username {1} --password {2} --query \"{3}\" --hbase-create-table --hbase-table {4} --column-family {5} --hive-drop-import-delims --null-string {6} --null-non-string {7} --hbase-row-key {8} --m {9}";
	
	public static final String SQOOP_ORACLE_2_HBASE = "sqoop import --connect \"{0}\" --username {1} --password {2} --table {3} --hbase-create-table --hbase-table {4} --column-family {5} --hive-drop-import-delims --null-string {6} --null-non-string {7} --hbase-row-key {8} --m {9}";

	public static final String SQOOP_TERADATA_2_HBASE_BY_SQL = "sqoop import --driver {0} --connect {1} --username {2} --password {3} --query \"{4}\" --hbase-create-table --hbase-table {5} --column-family {6} --hive-drop-import-delims --null-string {7} --null-non-string {8} --hbase-row-key {9} --m {10}";
	
	public static final String SQOOP_TERADATA_2_HBASE = "sqoop import --driver {0} --connect {1} --username {2} --password {3} --table {4} --hbase-create-table --hbase-table {5} --column-family {6} --hive-drop-import-delims --null-string {7} --null-non-string {8} --hbase-row-key {9} --m {10}";
	
	public static final String SQOOP_TERADATA_2_HDFS_BY_SQL = "sqoop import --driver {0} --connect {1} --username {2} --password {3} --query \"{4}\" --target-dir {5} --null-string {6} --null-non-string {7} --fields-terminated-by {8} --append --m {9}";

	public static final String SQOOP_TERADATA_2_HDFS = "sqoop import --driver {0} --connect {1} --username {2} --password {3} --table {4} --target-dir {5} --null-string {6} --null-non-string {7} --fields-terminated-by {8} --append --m {9}";
}
