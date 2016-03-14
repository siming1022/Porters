package com.teamsun.porters.move.op;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.util.HdfsUtils;
import com.teamsun.porters.move.util.StringUtils;

public class Hdfs2HdfsOp extends MoveOpration
{
	private static Logger log = LoggerFactory.getLogger(Hdfs2HdfsOp.class);
	private HdfsUtils hdfsUtils = new HdfsUtils();
	
	public Hdfs2HdfsOp(){}
	
	public Hdfs2HdfsOp(String type, ConfigDomain configDto)
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

		if (StringUtils.isEmpty(configDto.getDestHdfsLoc()))
		{
			throw new BaseException("目的HDFS地址不能为空");
		}
		
		String sourceHdfsLoc = configDto.getSourceHdfsLoc();
		String destHdfsLoc = configDto.getDestHdfsLoc();
		
		if (sourceHdfsLoc.equals(destHdfsLoc))
		{
			throw new BaseException("源HDFS地址与目的HDFS地址不能一致");
		}
	}

	@Override
	public void move() throws BaseException 
	{
		log.info("begin to from hdfs to hdfs");
		String command = hdfsUtils.genCopyCommand(configDto.getSourceHdfsLoc(), configDto.getDestHdfsLoc());
		String res = runCommand(command);
		log.info("hdfs 2 hdsf res: " + res);
		log.info("from hdfs to hdfs finish");
	}
}
