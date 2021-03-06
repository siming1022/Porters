package com.teamsun.porters.move.util;

/**
 * 常量类
 * 
 * @author Administrator
 *
 */
public class Constants 
{
	public static final String SQOOP_FIELDS_TERMINATED_BY = "'\\t'";
	
//	public static final String SQOOP_INPUT_NULL_STRING = "'\\\\N'"; 

	public static final String SQOOP_INPUT_NULL_STRING = "''"; 
	
	public static final String SQOOP_NULL_STRING = "'\\\\N'"; 

//	public static final String SQOOP_NULL_NON_STRING = "'\\\\N'";

	public static final String SQOOP_NULL_NON_STRING = "''";
	
	public static final String DATA_MOVE_TYPE_H2H = "HDFS2HDFS";

	public static final String DATA_MOVE_TYPE_H2O = "HDFS2ORACLE";

	public static final String DATA_MOVE_TYPE_H2T = "HDFS2TERADATA";

	public static final String DATA_MOVE_TYPE_H2V = "HDFS2VERTICA";

	public static final String DATA_MOVE_TYPE_H2HB = "HDFS2HBASE";
	
	public static final String DATA_MOVE_TYPE_H2M = "HDFS2MYSQL";
	
	public static final String DATA_MOVE_TYPE_T2H = "TERADATA2HDFS";
	
	public static final String DATA_MOVE_TYPE_T2HB = "TERADATA2HBASE";
	
	public static final String DATA_MOVE_TYPE_O2H = "ORACLE2HDFS";
	
	public static final String DATA_MOVE_TYPE_O2HB = "ORACLE2HBASE";
	
	public static final String DATA_TYPE_HDFS = "HDFS";

	public static final String DATA_TYPE_ORACLE = "ORACLE";
	
	public static final String DATA_TYPE_TERADATA = "TERADATA";
	
	public static final String DATA_TYPE_VERTICA = "VERTICA";
	
	public static final String DATA_TYPE_HBASE = "HBASE";
	
	public static final String DATA_TYPE_MYSQL = "MYSQL";
	
	public static final String DB_DRIVER_CLASS_ORACLE = "oracle.jdbc.driver.OracleDriver";

	public static final String DB_DRIVER_CLASS_TERADATA = "com.ncr.teradata.TeraDriver";

	public static final String DB_DRIVER_CLASS_VERTICA = "com.vertica.jdbc.Driver";

	public static final String DB_DRIVER_CLASS_MYSQL = "com.mysql.jdbc.Driver";

	public static final String DB_DRIVER_CLASS_HIVE = "org.apache.hive.jdbc.HiveDriver";
	
	public static final String HIVE_EXT_DATABASE_NAME_TEST = "TEST";
	
	public static final String HIVE_EXT_DATABASE_NAME_PDATA = "EMS_PDATA";
	
	public static final String HBASE_ROWKEY_SPLIT = "~";
	
	public static final String DATA_SPLIT = "\t";

	public static final String CONFIG_HBASE_ROWKEY_PROTECTED = "hbase.rowkey.protected";
	
	public static final String OUT_PATH_TEMP = "/EMS_Data/teamsun/tmp";
	
	public static final String DATA_TYPE_VARCHAR = "VARCHAR";
	
	public static final String DATA_TYPE_CHAR = "CHAR";
	
	public static final String DATA_TYPE_LONG = "LONG";

	public static final String DATA_TYPE_NUMBER = "NUMBER";
	
	public static final String DATA_TYPE_DATE = "DATE";
	
	public static final String DATA_TYPE_TIMESTAMP = "TIMESTAMP";
}
