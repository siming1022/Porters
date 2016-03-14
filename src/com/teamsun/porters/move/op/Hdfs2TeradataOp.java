package com.teamsun.porters.move.op;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.server.MoveServer;
import com.teamsun.porters.move.util.StringUtils;

public class Hdfs2TeradataOp extends MoveOpration
{
	private static Logger log = LoggerFactory.getLogger(Hdfs2TeradataOp.class);
	
	
	public Hdfs2TeradataOp(){}
	
	public Hdfs2TeradataOp(String type, ConfigDomain configDto)
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
	public void move()
			throws BaseException {
		// TODO Auto-generated method stub
		
	}
}
