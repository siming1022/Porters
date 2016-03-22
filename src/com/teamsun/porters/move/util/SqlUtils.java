package com.teamsun.porters.move.util;

import java.text.MessageFormat;

import com.teamsun.porters.move.domain.DBMoveDomain;
import com.teamsun.porters.move.domain.HbaseDto;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.table.ColumnsDto;
import com.teamsun.porters.move.domain.table.TableDto;
import com.teamsun.porters.move.template.SqlTemplate;

/**
 * 生成Sql帮助类
 * 
 * @author Administrator
 *
 */
public class SqlUtils 
{
	public static String genHiveTxtTableSql(HdfsDto srcDto, DBMoveDomain destDto)
	{
		
		StringBuffer sb = new StringBuffer(" drop table if exists " + Constants.HIVE_EXT_DATABASE_NAME + "." + destDto.getTableName().toUpperCase() + "; \n");
		sb.append("create EXTERNAL table " + Constants.HIVE_EXT_DATABASE_NAME + "." + destDto.getTableName().toUpperCase() + "(");
		for (ColumnsDto colDto : destDto.getTableDto().getColumnList()) 
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
        sb.append("FIELDS TERMINATED BY \'\\t\'" + "\n");
        sb.append("STORED AS INPUTFORMAT " + "\n");
        sb.append("'org.apache.hadoop.mapred.TextInputFormat'" + "\n");
        sb.append("OUTPUTFORMAT" + "\n");
        sb.append("'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \n");
        sb.append("LOCATION \n");
        sb.append("'" + srcDto.getHdfsLoc() + "'; \n");
        
		return sb.toString();
	}
	
	public static String getBulkloadSql(DBMoveDomain dto)
	{
		StringBuffer sb = new StringBuffer("insert into " + dto.getDatabaseName() + "." + dto.getTableName() + " SELECT /*+USE_BULKLOAD*/ ");
		
		for (ColumnsDto colDto : dto.getTableDto().getColumnList())
		{
			sb.append(colDto.getColumnName() + ", ");
		}
		
		sb = new StringBuffer(sb.toString().substring(0, sb.length() - 2));
		
		sb.append(" FROM TEST." + dto.getTableName());
		
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

	public static String genDelHiveTxtTableSql(DBMoveDomain destDto) 
	{
		return MessageFormat.format(SqlTemplate.HIVE_DROP_TABLE_SQL, Constants.HIVE_EXT_DATABASE_NAME, destDto.getTableName());
	}
}
