package com.teamsun.porters.move.template;

/**
 * Sqoop命令模板类
 * 
 * @author Administrator
 *
 */
public class SqoopCommandTemplate 
{
	public static final String SQOOP_HDFS_2_ORACLE = "sqoop export --connect {0} --username {1} --password {2} --table {3} --export-dir {4} --fields-terminated-by {5} --input-null-string {6} --null-non-string {7} --m {8}";
	
	public static final String SQOOP_HDFS_2_TERADATA = "sqoop export --connect {0} --driver{1} --username {2} --password {3} --table {4} --export-dir {5}";

	public static final String SQOOP_HDFS_2_VERTICA = "sqoop export --connect {0} --username {1} --password {2} --table {3} --export-dir {4} --fields-terminated-by {5} --input-null-string {6} --null-non-string {7} --m {8}";

	public static final String SQOOP_HDFS_2_MYSQL = "sqoop export --connect {0} --username {1} --password {2} --table {3} --export-dir {4} --fields-terminated-by {5} --input-null-string {6} --null-non-string {7} --m {8}";

	public static final String SQOOP_ORACLE_2_HDFS = "";
	
	public static final String SQOOP_TERADATA_2_HDFS = "";
}
