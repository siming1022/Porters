package com.teamsun.porters.move.factory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HbaseDto;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.MySqlDto;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.VerticaDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.op.Hdfs2HbaseOp;
import com.teamsun.porters.move.op.Hdfs2HdfsOp;
import com.teamsun.porters.move.op.Hdfs2MySqlOp;
import com.teamsun.porters.move.op.Hdfs2OracleOp;
import com.teamsun.porters.move.op.Hdfs2TeradataOp;
import com.teamsun.porters.move.op.Hdfs2VerticaOp;
import com.teamsun.porters.move.util.Constants;

public class MoveDtoFactory 
{
	public BaseMoveDomain createSrcDto(ConfigDomain configDto)
	{
		String dataSource = configDto.getDataSource().toUpperCase();
		
		if (Constants.DATA_TYPE_HDFS.equals(dataSource))
		{
			HdfsDto dto = new HdfsDto();
			dto.setHdfsLoc(configDto.getSourceHdfsLoc());
			
			return dto;
		}
		else
		{
			return null;
		}
	}

	public BaseMoveDomain createDestDto(ConfigDomain configDto)
	{
		String dataDest = configDto.getDataDest().toUpperCase();
		
		if (Constants.DATA_TYPE_HDFS.equals(dataDest))
		{
			HdfsDto dto = new HdfsDto();
			dto.setHdfsLoc(configDto.getDestHdfsLoc());
			
			return dto;
		}
		else if (Constants.DATA_TYPE_ORACLE.equals(dataDest))
		{
			OracleDto dto = new OracleDto();
			
			return dto;
		}
		else if (Constants.DATA_TYPE_TERADATA.equals(dataDest))
		{
			TeradataDto dto = new TeradataDto();
			
			return dto;
		}
		else if (Constants.DATA_TYPE_VERTICA.equals(dataDest))
		{
			VerticaDto dto = new VerticaDto();
			
			return dto;
		}
		else if (Constants.DATA_TYPE_HBASE.equals(dataDest))
		{
			HbaseDto dto = new HbaseDto();
			
			return dto;
		}
		else if (Constants.DATA_TYPE_MYSQL.equals(dataDest))
		{
			MySqlDto dto = new MySqlDto();
			
			return dto;
		}
		else
		{
			return null;
		}
	}
}
