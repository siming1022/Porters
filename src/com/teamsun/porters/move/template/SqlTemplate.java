package com.teamsun.porters.move.template;

public class SqlTemplate 
{
	public static final String HIVE_DROP_TABLE_SQL = "DROP TABLE IF EXISTS {0}.{1}";

	public static final String ORACLE_QUERY_TABLE_PARTITION_COL_SQL = "SELECT * FROM USER_PART_KEY_COLUMNS WHERE OBJECT_TYPE = ''TABLE''  AND NAME = ''{0}''";

	public static final String ORACLE_QUERY_TABLE_PARTITION_SQL = "SELECT {0} AS VALUE FROM {1} WHERE {2} GROUP BY {3} ORDER BY {4}";

	public static final String ORACLE_QUERY_TABLE_PARTITION_NO_TIME_SQL = "SELECT {0} AS VALUE FROM {1} GROUP BY {2} ORDER BY {3}";
}
