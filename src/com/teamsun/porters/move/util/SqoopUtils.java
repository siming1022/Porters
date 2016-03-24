package com.teamsun.porters.move.util;

import java.text.MessageFormat;

import com.teamsun.porters.move.domain.HbaseDto;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.MySqlDto;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.VerticaDto;
import com.teamsun.porters.move.template.SqoopCommandTemplate;

public class SqoopUtils 
{
	public static String genExportFromHdfsToOralce(BaseMoveDomain srcMd, BaseMoveDomain destMd)
	{
		HdfsDto srcDto = (HdfsDto) srcMd;
		OracleDto destDto = (OracleDto) destMd;
		
		//String sqoopBaseStr = "sqoop export --connect {0} --username {1} --password {2} --table {3} --export-dir {4} --fields-terminated-by {5} --input-null-string {6} --null-non-string {7} --m {8}";
		
		return MessageFormat.format(SqoopCommandTemplate.SQOOP_HDFS_2_ORACLE, 
				StringUtils.isEmpty(destDto.getTns())?destDto.getJdbcUrl():destDto.getTns(), destDto.getUserName(), destDto.getPasswd(), 
				destDto.getTableName(), srcDto.getHdfsLoc(), StringUtils.getValue(srcDto.getFieldsTerminatedBy(), Constants.SQOOP_FIELDS_TERMINATED_BY),
				StringUtils.getValue(srcDto.getInputNullString(), Constants.SQOOP_INPUT_NULL_STRING), StringUtils.getValue(srcDto.getNullNonString(), Constants.SQOOP_NULL_NON_STRING)
				, "32");
	}

	public static String genExportFromHdfsToTeradata(BaseMoveDomain srcMd, BaseMoveDomain destMd)
	{
		HdfsDto srcDto = (HdfsDto) srcMd;
		TeradataDto destDto = (TeradataDto) destMd;
		
		return MessageFormat.format(SqoopCommandTemplate.SQOOP_HDFS_2_TERADATA, 
				destDto.getJdbcUrl(), Constants.DB_DRIVER_CLASS_TERADATA, destDto.getUserName(), destDto.getPasswd(), 
				destDto.getTableName(), srcDto.getHdfsLoc(), StringUtils.getValue(srcDto.getFieldsTerminatedBy(), Constants.SQOOP_FIELDS_TERMINATED_BY), "32");
	}

	public static String genExportFromHdfsToVertica(BaseMoveDomain srcMd, BaseMoveDomain destMd)
	{
		HdfsDto srcDto = (HdfsDto) srcMd;
		VerticaDto destDto = (VerticaDto) destMd;
		
		//sqoop export --connect {0} --driver{1} --username {2} --password {3} --table {4} --export-dir {5}
		
		return MessageFormat.format(SqoopCommandTemplate.SQOOP_HDFS_2_VERTICA, 
				destDto.getJdbcUrl(), Constants.DB_DRIVER_CLASS_VERTICA, destDto.getUserName(), destDto.getPasswd(), 
				destDto.getTableName(), srcDto.getHdfsLoc());
	}

	public static String genExportFromHdfsToMySql(BaseMoveDomain srcMd, BaseMoveDomain destMd)
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
					StringUtils.isEmpty(srcDto.getTns())?srcDto.getJdbcUrl():srcDto.getTns(), srcDto.getUserName(), srcDto.getPasswd(), srcDto.getTableName(), destDto.getHdfsLoc(),
					StringUtils.getValue(destDto.getInputNullString(), Constants.SQOOP_INPUT_NULL_STRING), StringUtils.getValue(destDto.getNullNonString(), Constants.SQOOP_NULL_NON_STRING), 
					Constants.SQOOP_FIELDS_TERMINATED_BY, "1");
		}
		else
		{
			command = MessageFormat.format(SqoopCommandTemplate.SQOOP_ORACLE_2_HDFS_BY_SQL,
					StringUtils.isEmpty(srcDto.getTns())?srcDto.getJdbcUrl():srcDto.getTns(), srcDto.getUserName(), srcDto.getPasswd(), srcDto.getQuerySql(), destDto.getHdfsLoc(),
					StringUtils.getValue(destDto.getInputNullString(), Constants.SQOOP_INPUT_NULL_STRING), StringUtils.getValue(destDto.getNullNonString(), Constants.SQOOP_NULL_NON_STRING), 
					Constants.SQOOP_FIELDS_TERMINATED_BY, "1");
		}
		
		return command;
	}

	public static String genImportFromOralceToHbase(BaseMoveDomain srcMd, BaseMoveDomain destMd, boolean isAll) 
	{
		OracleDto srcDto = (OracleDto) srcMd;
		HbaseDto destDto = (HbaseDto) destMd;
		
		String command = null;
		if (isAll)
		{
			command = MessageFormat.format(SqoopCommandTemplate.SQOOP_ORACLE_2_HBASE,
					StringUtils.isEmpty(srcDto.getTns())?srcDto.getJdbcUrl():srcDto.getTns(), srcDto.getUserName(), srcDto.getPasswd(), srcDto.getTableName(), destDto.getTableName(),
					destDto.getColFamily(), Constants.SQOOP_NULL_STRING, Constants.SQOOP_NULL_NON_STRING, destDto.getRowkeys(), "1");
		}
		else
		{
			command = MessageFormat.format(SqoopCommandTemplate.SQOOP_ORACLE_2_HBASE_BY_SQL,
					StringUtils.isEmpty(srcDto.getTns())?srcDto.getJdbcUrl():srcDto.getTns(), srcDto.getUserName(), srcDto.getPasswd(), srcDto.getQuerySql(), destDto.getTableName(),
					destDto.getColFamily(), Constants.SQOOP_NULL_STRING, Constants.SQOOP_NULL_NON_STRING, destDto.getRowkeys(), "1");
		}
		return command;
	}

	public static String genImportFromTeradataToHbase(BaseMoveDomain srcMd, BaseMoveDomain destMd, boolean isAll) 
	{
		TeradataDto srcDto = (TeradataDto) srcMd;
		HbaseDto destDto = (HbaseDto) destMd;
		
		String command = null;
		if (isAll)
		{
			command = MessageFormat.format(SqoopCommandTemplate.SQOOP_TERADATA_2_HBASE,
					Constants.DB_DRIVER_CLASS_TERADATA, srcDto.getJdbcUrl(), srcDto.getUserName(), srcDto.getPasswd(), srcDto.getTableName(), destDto.getTableName(),
					destDto.getColFamily(), Constants.SQOOP_NULL_STRING, Constants.SQOOP_NULL_NON_STRING, "1");
		}
		else
		{
			command = MessageFormat.format(SqoopCommandTemplate.SQOOP_TERADATA_2_HBASE_BY_SQL,
					Constants.DB_DRIVER_CLASS_TERADATA, srcDto.getJdbcUrl(), srcDto.getUserName(), srcDto.getPasswd(), srcDto.getQuerySql(), destDto.getTableName(),
					destDto.getColFamily(), Constants.SQOOP_NULL_STRING, Constants.SQOOP_NULL_NON_STRING, "1");
		}
		return command;
	}

	public static String genImportFromTeradataToHdfs(BaseMoveDomain srcMd,	BaseMoveDomain destMd, boolean isAll) 
	{
		TeradataDto srcDto = (TeradataDto) srcMd;
		HdfsDto destDto = (HdfsDto) destMd;
		
		String command = null;
		if (isAll)
		{
			command = MessageFormat.format(SqoopCommandTemplate.SQOOP_TERADATA_2_HDFS,
					srcDto.getDriverClass(), srcDto.getJdbcUrl(), srcDto.getUserName(), srcDto.getPasswd(), 
					srcDto.getTableName(), destDto.getHdfsLoc(), Constants.SQOOP_NULL_STRING, Constants.SQOOP_NULL_NON_STRING, 
					Constants.SQOOP_FIELDS_TERMINATED_BY, "1");
		}
		else
		{
			command = MessageFormat.format(SqoopCommandTemplate.SQOOP_TERADATA_2_HDFS_BY_SQL,
					srcDto.getDriverClass(), srcDto.getJdbcUrl(), srcDto.getUserName(), srcDto.getPasswd(), 
					srcDto.getQuerySql(), destDto.getHdfsLoc(), Constants.SQOOP_NULL_STRING, Constants.SQOOP_NULL_NON_STRING, 
					Constants.SQOOP_FIELDS_TERMINATED_BY, "1");
		}
		return command;
	}
}
