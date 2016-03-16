package com.teamsun.porters.move.op.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.op.hdfs.Hdfs2HdfsOp;
import com.teamsun.porters.move.util.HdfsUtils;

public class Oralce2HbaseOp extends MoveOpration
{
	private static Logger log = LoggerFactory.getLogger(Oralce2HbaseOp.class);
	
	public Oralce2HbaseOp(){}
	
	public Oralce2HbaseOp(String type, ConfigDomain configDto)
	{
		super(type, configDto);
	}

	@Override
	public void move() throws BaseException 
	{
		
	}

	@Override
	public void valid() throws BaseException 
	{
		
	}
}
