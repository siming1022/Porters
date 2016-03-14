package com.teamsun.porters.move.op;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
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
//		Connection conn = DBMSMetaUtil.getConnection(dto.getDriverClass(), dto.getJdbcUrl(), dto.getUserName(), dto.getPasswd());
//		Statement stm = conn.createStatement();
//		ResultSet rs = stm.executeQuery("");
		
//		String createExtTableSql = SqlUtils.genHiveTxtTableSql(dto);
		//2.建一张Txt外表
		//3.通过BulkLoad导入至Hbase
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
		if (StringUtils.isEmpty(configDto.getDestDBName()))
		{
			throw new BaseException("目的数据库名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestDBUserName()))
		{
			throw new BaseException("目的数据库用户名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestDBPwd()))
		{
			throw new BaseException("目的数据库密码不能为空");
		}
	}
	
}
