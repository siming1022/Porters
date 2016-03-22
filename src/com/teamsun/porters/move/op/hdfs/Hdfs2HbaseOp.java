package com.teamsun.porters.move.op.hdfs;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.HbaseDto;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.factory.MoveDtoFactory;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.util.DBMSMetaUtil;
import com.teamsun.porters.move.util.SqlUtils;
import com.teamsun.porters.move.util.StringUtils;

public class Hdfs2HbaseOp extends MoveOpration
{
	private static Logger log = LoggerFactory.getLogger(Hdfs2HbaseOp.class);
	
	
	public Hdfs2HbaseOp(){}
	
	public Hdfs2HbaseOp(String type, ConfigDomain configDto)
	{
		super(type, configDto);
	}

	@Override
	public void move() throws BaseException 
	{
		//1.通过Java JDBC连接Hive Server2
		Connection conn = null;
		Statement stm = null;
		ResultSet rs = null;
		try 
		{
			HdfsDto srcDto = (HdfsDto) MoveDtoFactory.createSrcDto(configDto);
			HbaseDto destDto = (HbaseDto) MoveDtoFactory.createDestDto(configDto);
			
			String createExtTableSql = SqlUtils.genHiveTxtTableSql(srcDto, destDto);
			conn = DBMSMetaUtil.getConnection(destDto.getDriverClass(), destDto.getJdbcUrl(), destDto.getUserName(), destDto.getPasswd());
			stm = conn.createStatement();
			rs = stm.executeQuery(createExtTableSql);
			
			String bulkloadCommand = SqlUtils.getBulkloadSql(destDto);

			stm = conn.createStatement();
			rs = stm.executeQuery(bulkloadCommand);
			
			String deleteExtTableSql = SqlUtils.genDelHiveTxtTableSql(destDto);
			stm = conn.createStatement();
			rs = stm.executeQuery(deleteExtTableSql);
			
			conn.commit();
		}
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			DBMSMetaUtil.close(rs);
			DBMSMetaUtil.close(stm);
			DBMSMetaUtil.close(conn);
		}
	}

	@Override
	public void valid() throws BaseException
	{
		if (StringUtils.isEmpty(configDto.getSourceHdfsLoc()))
		{
			throw new BaseException("源HDFS地址不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestTable()))
		{
			throw new BaseException("目的表名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestDBIp()))
		{
			throw new BaseException("目的数据库IP不能为空");
		}
	}
	
}
