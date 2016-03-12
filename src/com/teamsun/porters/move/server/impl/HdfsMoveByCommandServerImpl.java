package com.teamsun.porters.move.server.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HbaseDto;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.VerticaDto;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.server.MoveServer;
import com.teamsun.porters.move.util.HdfsUtils;
import com.teamsun.porters.move.util.SqlUtils;
import com.teamsun.porters.move.util.SqoopUtils;

public class HdfsMoveByCommandServerImpl implements MoveServer 
{
	private static Logger log = LoggerFactory.getLogger(HdfsMoveByCommandServerImpl.class);
	private SqoopUtils sqoopUtils = new SqoopUtils();
	private HdfsUtils hdfsUtils = new HdfsUtils();
	
	public HdfsMoveByCommandServerImpl() 
	{
		super();
	}
	
	@Override
	public void move2Hdfs(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException
	{
		HdfsDto dto = (HdfsDto) destDomain;
		
		if (!srcDomain.equals(dto))
		{
			log.info("begin to from hdfs to hdfs");
			String command = hdfsUtils.genCopyCommand(srcDomain, dto);
			Runtime.getRuntime().exec(command);
			log.info("from hdfs to hdfs finish");
		}
		else
		{
			log.info("src location eq dest location");
		}
	}

	@Override
	public void move2Td(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException
	{
		TeradataDto dto = (TeradataDto) destDomain;
		
		String sqoopCommand = sqoopUtils.genExportToTeradata(srcDomain, dto);
		
		log.info("begin to from hdfs to teradata");
		String command = sqoopCommand;
		Runtime.getRuntime().exec(command);
		log.info("from hdfs to teradata finish");
	}

	@Override
	public void move2Oracle(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException
	{
		OracleDto dto = (OracleDto) destDomain;
		String sqoopCommand = sqoopUtils.genExportToOralce(srcDomain, dto);
		
		log.info("begin to from hdfs to oracle");
		String command = sqoopCommand;
		Runtime.getRuntime().exec(command);
		log.info("from hdfs to oracle finish");
	}

	@Override
	public void move2Hbase(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException
	{
		HbaseDto dto = (HbaseDto) destDomain;
		//1.通过Java JDBC连接Hive Server2
//		Connection conn = DBMSMetaUtil.getConnection(dto.getDriverClass(), dto.getJdbcUrl(), dto.getUserName(), dto.getPasswd());
//		Statement stm = conn.createStatement();
//		ResultSet rs = stm.executeQuery("");
		
		String createExtTableSql = SqlUtils.genHiveTxtTableSql(dto);
		//2.建一张Txt外表
		//3.通过BulkLoad导入至Hbase
	}

	@Override
	public void move2Vertica(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException
	{
		VerticaDto dto = (VerticaDto) destDomain;
	}

}
