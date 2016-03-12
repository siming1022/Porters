package com.teamsun.porters.move.util;

import java.text.MessageFormat;

import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.template.SqoopCommandTemplate;

public class SqoopUtils 
{
	public String genExportToOralce(BaseMoveDomain srcMd, BaseMoveDomain destMd)
	{
		HdfsDto srcDto = (HdfsDto) srcMd;
		OracleDto destDto = (OracleDto) destMd;
		
		//String sqoopBaseStr = "sqoop export --connect {0} --username {1} --password {2} --table {3} --export-dir {4} --fields-terminated-by {5} --input-null-string {6} --null-non-string {7} --m {8}";
		
		return MessageFormat.format(SqoopCommandTemplate.SQOOP_HDFS_2_ORACLE, 
				destDto.getJdbcUrl(), destDto.getUserName(), destDto.getPasswd(), 
				destDto.getTableName(), srcDto.getHdfsLoc(), StringUtils.getValue(srcDto.getFieldsTerminatedBy(), Constants.SQOOP_FIELDS_TERMINATED_BY),
				StringUtils.getValue(srcDto.getInputNullString(), Constants.SQOOP_INPUT_NULL_STRING), StringUtils.getValue(srcDto.getNullNonString(), Constants.SQOOP_NULL_NON_STRING));
	}

	public String genExportToTeradata(BaseMoveDomain srcMd, BaseMoveDomain destMd)
	{
		HdfsDto srcDto = (HdfsDto) srcMd;
		TeradataDto destDto = (TeradataDto) destMd;
		
		//sqoop export --connect {0} --driver{1} --username {2} --password {3} --table {4} --export-dir {5}
		
		return MessageFormat.format(SqoopCommandTemplate.SQOOP_HDFS_2_TERADATA, 
				destDto.getJdbcUrl(), Constants.TERADATA_DRIVER, destDto.getUserName(), destDto.getPasswd(), 
				destDto.getTableName(), srcDto.getHdfsLoc());
	}
}
