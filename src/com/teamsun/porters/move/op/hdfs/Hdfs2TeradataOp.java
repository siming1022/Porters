package com.teamsun.porters.move.op.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.exe.MoveMain;
import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.factory.MoveDtoFactory;
import com.teamsun.porters.move.mapper.DataMoveMapper;
import com.teamsun.porters.move.op.MoveOpration;
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
	public void move() throws BaseException 
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		log.info("begin to from hdfs to teradata");
		runMapReduce(srcDto, destDto);
		log.info("from hdfs to teradata finish");
	}

	/*public void move() throws BaseException 
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		String sqoopCommand = SqoopUtils.genExportFromHdfsToTeradata(srcDto, destDto);
		
		log.info("begin to from hdfs to teradata");
		String command = sqoopCommand;
		String res = runCommand(command);
		log.info("run command res: " + res);
		log.info("from hdfs to teradata finish");
	}*/
}
