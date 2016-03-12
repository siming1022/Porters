package com.teamsun.porters.move.util;

import com.teamsun.porters.move.domain.DBMoveDomain;
import com.teamsun.porters.move.domain.table.ColumnsDto;

/**
 * 生成Sql帮助类
 * 
 * @author Administrator
 *
 */
public class SqlUtils 
{
	public static String genHiveTxtTableSql(DBMoveDomain dbDomain)
	{
		
		StringBuffer sb = new StringBuffer(" drop table if exists " + dbDomain.getDatabaseName().toUpperCase() + "." + dbDomain.getTableName().toUpperCase() + "; \n");
		
		for (ColumnsDto colDto : dbDomain.getTableDto().getColumnList()) 
        {
    		sb.append(colDto.getColumnName() + " " + changeType(colDto.getSqlType().toUpperCase()) + "," + " \n");
        }
        
        sb = new StringBuffer(sb.substring(0, sb.length() - 3));
        sb.append(") " + "\n");
		
		/**
         * 建文件文件外表
         *
         **/
        sb.append("ROW FORMAT DELIMITED  " + "\n");
        sb.append("FIELDS TERMINATED BY \'\\,\'" + "\n");
        sb.append("STORED AS INPUTFORMAT " + "\n");
        sb.append("'org.apache.hadoop.mapred.TextInputFormat'" + "\n");
        sb.append("OUTPUTFORMAT" + "\n");
        sb.append("'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \n");
        sb.append("LOCATION \n");
        sb.append("'/EMS_Data/teamsun/temp/" + dbDomain.getTableName().toUpperCase() + "'; \n");
		return sb.toString();
	}
	
	private static String changeType(String type) 
	{
		if (type.contains("VARCHAR") || type.contains("CHAR") || type.contains("DATE"))
		{
			return "string";
		}
		else if (type.contains("INTEGER"))
		{
			return "int";
		}
		else if (type.contains("DECIMAL"))
		{
			return "double";
		}
		else
		{
			return "string";
		}
	}
}
