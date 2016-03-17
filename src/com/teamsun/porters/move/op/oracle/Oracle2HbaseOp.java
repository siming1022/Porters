package com.teamsun.porters.move.op.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.factory.MoveDtoFactory;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.util.SqoopUtils;
import com.teamsun.porters.move.util.StringUtils;

public class Oracle2HbaseOp extends MoveOpration
{
	private static Logger log = LoggerFactory.getLogger(Oracle2HbaseOp.class);
	
	public Oracle2HbaseOp(){}
	
	public Oracle2HbaseOp(String type, ConfigDomain configDto)
	{
		super(type, configDto);
	}

	@Override
	public void move() throws BaseException 
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		OracleDto oracleDto = (OracleDto)srcDto;
		String sqoopCommand = null;
		
		if (StringUtils.isEmpty(oracleDto.getQuerySql()) && StringUtils.isEmpty(oracleDto.getColumns()))
		{
			sqoopCommand = SqoopUtils.genImportFromOralceToHbase(srcDto, destDto, true);
		}
		else
		{
			if (StringUtils.isEmpty(oracleDto.getQuerySql()))
			{
				String querySql = "SELECT " + oracleDto.getColumns() + " FROM " + oracleDto.getTableName() + " WHERE \\$CONDITIONS";
				oracleDto.setQuerySql(querySql);
			}
			
			sqoopCommand = SqoopUtils.genImportFromOralceToHbase(oracleDto, destDto, false);
		}
		
		
		log.info("begin to from oracle to hbase");
		String command = sqoopCommand;
		String res = runCommand(command);
		log.info("run command res: " + res);
		log.info("from oracle to hbase finish");
	}

	@Override
	public void valid() throws BaseException 
	{
		if (StringUtils.isEmpty(configDto.getSourceTable()))
		{
			throw new BaseException("源表名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestTable()))
		{
			throw new BaseException("目的表名不能为空");
		}
		
		if (StringUtils.isEmpty(configDto.getSourceDBTns()))
		{
			if (StringUtils.isEmpty(configDto.getSourceDBIp()))
			{
				throw new BaseException("源数据库IP不能为空");
			}
			if (StringUtils.isEmpty(configDto.getSourceDBName()))
			{
				throw new BaseException("源数据库名不能为空");
			}
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
