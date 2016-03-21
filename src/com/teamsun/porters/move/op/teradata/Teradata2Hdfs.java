package com.teamsun.porters.move.op.teradata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.factory.MoveDtoFactory;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.op.oracle.Oracle2HbaseOp;
import com.teamsun.porters.move.util.SqoopUtils;
import com.teamsun.porters.move.util.StringUtils;

public class Teradata2Hdfs extends MoveOpration 
{
private static Logger log = LoggerFactory.getLogger(Teradata2Hdfs.class);
	
	public Teradata2Hdfs(){}
	
	public Teradata2Hdfs(String type, ConfigDomain configDto)
	{
		super(type, configDto);
	}

	@Override
	public void move() throws BaseException 
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		TeradataDto teradataDto = (TeradataDto)srcDto;
		String sqoopCommand = null;
		
		if (StringUtils.isEmpty(teradataDto.getQuerySql()) && StringUtils.isEmpty(teradataDto.getColumns()))
		{
			sqoopCommand = SqoopUtils.genImportFromTeradataToHdfs(srcDto, destDto, true);
		}
		else
		{
			if (StringUtils.isEmpty(teradataDto.getQuerySql()))
			{
				String querySql = "SELECT " + teradataDto.getColumns() + " FROM " + teradataDto.getDatabaseName() + "." + teradataDto.getTableName() + " WHERE \\$CONDITIONS";
				teradataDto.setQuerySql(querySql);
			}
			
			sqoopCommand = SqoopUtils.genImportFromTeradataToHdfs(teradataDto, destDto, false);
		}
		
		
		log.info("begin to from teradata to hdfs");
		String command = sqoopCommand;
		String res = runCommand(command);
		log.info("run command res: " + res);
		log.info("from teradata to hbase hdfs");
	}

	@Override
	public void valid() throws BaseException 
	{
		if (StringUtils.isEmpty(configDto.getSourceTable()))
		{
			throw new BaseException("源表名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestHdfsLoc()))
		{
			throw new BaseException("目的地址不能为空");
		}
		if (StringUtils.isEmpty(configDto.getSourceDBIp()))
		{
			throw new BaseException("源数据库IP不能为空");
		}
		if (StringUtils.isEmpty(configDto.getSourceDBName()))
		{
			throw new BaseException("源数据库名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getSourceDBUserName()))
		{
			throw new BaseException("源数据库用户名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getSourceDBPwd()))
		{
			throw new BaseException("源数据库密码不能为空");
		}
	}
}
