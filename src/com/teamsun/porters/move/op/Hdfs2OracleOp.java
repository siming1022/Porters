package com.teamsun.porters.move.op;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.util.SqoopUtils;
import com.teamsun.porters.move.util.StringUtils;

public class Hdfs2OracleOp extends MoveOpration
{
	private static Logger log = LoggerFactory.getLogger(Hdfs2OracleOp.class);
	private SqoopUtils sqoopUtils = new SqoopUtils();
	
	public Hdfs2OracleOp(){}
	
	public Hdfs2OracleOp(String type, ConfigDomain configDto)
	{
		super(type, configDto);
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

	@Override
	public void move() throws BaseException 
	{
		String sqoopCommand = sqoopUtils.genExportToOralce(srcDomain, dto);
		
		log.info("begin to from hdfs to oracle");
		String command = sqoopCommand;
		Runtime.getRuntime().exec(command);
		log.info("from hdfs to oracle finish");
	}
}
