package com.teamsun.porters.move.util;

import java.text.MessageFormat;

import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.MySqlDto;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.VerticaDto;
import com.teamsun.porters.move.template.SqoopCommandTemplate;

public class SqoopUtils 
{
	public static String genExportToOralce(BaseMoveDomain srcMd, BaseMoveDomain destMd)
	{
		HdfsDto srcDto = (HdfsDto) srcMd;
		OracleDto destDto = (OracleDto) destMd;
		
		//String sqoopBaseStr = "sqoop export --connect {0} --username {1} --password {2} --table {3} --export-dir {4} --fields-terminated-by {5} --input-null-string {6} --null-non-string {7} --m {8}";
		
		return MessageFormat.format(SqoopCommandTemplate.SQOOP_HDFS_2_ORACLE, 
				destDto.getJdbcUrl(), destDto.getUserName(), destDto.getPasswd(), 
				destDto.getTableName(), srcDto.getHdfsLoc(), StringUtils.getValue(srcDto.getFieldsTerminatedBy(), Constants.SQOOP_FIELDS_TERMINATED_BY),
				StringUtils.getValue(srcDto.getInputNullString(), Constants.SQOOP_INPUT_NULL_STRING), StringUtils.getValue(srcDto.getNullNonString(), Constants.SQOOP_NULL_NON_STRING));
	}

	public static String genExportToTeradata(BaseMoveDomain srcMd, BaseMoveDomain destMd)
	{
		HdfsDto srcDto = (HdfsDto) srcMd;
		TeradataDto destDto = (TeradataDto) destMd;
		
		//sqoop export --connect {0} --driver{1} --username {2} --password {3} --table {4} --export-dir {5}
		
		return MessageFormat.format(SqoopCommandTemplate.SQOOP_HDFS_2_TERADATA, 
				destDto.getJdbcUrl(), Constants.DB_DRIVER_CLASS_TERADATA, destDto.getUserName(), destDto.getPasswd(), 
				destDto.getTableName(), srcDto.getHdfsLoc());
	}

	public static String genExportToVertica(BaseMoveDomain srcMd, BaseMoveDomain destMd)
	{
		HdfsDto srcDto = (HdfsDto) srcMd;
		VerticaDto destDto = (VerticaDto) destMd;
		
		//sqoop export --connect {0} --driver{1} --username {2} --password {3} --table {4} --export-dir {5}
		
		return MessageFormat.format(SqoopCommandTemplate.SQOOP_HDFS_2_VERTICA, 
				destDto.getJdbcUrl(), Constants.DB_DRIVER_CLASS_VERTICA, destDto.getUserName(), destDto.getPasswd(), 
				destDto.getTableName(), srcDto.getHdfsLoc());
	}

	public static String genExportToMySql(BaseMoveDomain srcMd, BaseMoveDomain destMd)
	{
		HdfsDto srcDto = (HdfsDto) srcMd;
		MySqlDto destDto = (MySqlDto) destMd;
		
		//sqoop export --connect {0} --driver{1} --username {2} --password {3} --table {4} --export-dir {5}
		
		return MessageFormat.format(SqoopCommandTemplate.SQOOP_HDFS_2_MYSQL, 
				destDto.getJdbcUrl(), Constants.DB_DRIVER_CLASS_MYSQL, destDto.getUserName(), destDto.getPasswd(), 
				destDto.getTableName(), srcDto.getHdfsLoc());
	}

	public static String genImportFromOralceToHdfs(BaseMoveDomain srcMd, BaseMoveDomain destMd, boolean isAll) 
	{
		OracleDto srcDto = (OracleDto) srcMd;
		HdfsDto destDto = (HdfsDto) destMd;
		
		String command = null;
		if (isAll)
		{
			command = MessageFormat.format(SqoopCommandTemplate.SQOOP_ORACLE_2_HDFS,
					srcDto.getJdbcUrl(), srcDto.getUserName(), srcDto.getPasswd(), srcDto.getTableName(), destDto.getHdfsLoc(),
					StringUtils.getValue(destDto.getInputNullString(), Constants.SQOOP_INPUT_NULL_STRING), StringUtils.getValue(destDto.getNullNonString(), Constants.SQOOP_NULL_NON_STRING), "1");
		}
		else
		{
			command = MessageFormat.format(SqoopCommandTemplate.SQOOP_ORACLE_2_HDFS_BY_SQL,
					srcDto.getJdbcUrl(), srcDto.getUserName(), srcDto.getPasswd(), srcDto.getQuerySql(), destDto.getHdfsLoc(),
					StringUtils.getValue(destDto.getInputNullString(), Constants.SQOOP_INPUT_NULL_STRING), StringUtils.getValue(destDto.getNullNonString(), Constants.SQOOP_NULL_NON_STRING), "1");
		}
		
		return command;
	}
}
