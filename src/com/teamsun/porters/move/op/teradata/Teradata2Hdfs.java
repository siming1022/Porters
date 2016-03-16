package com.teamsun.porters.move.op.teradata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.op.oracle.Oralce2HbaseOp;

public class Teradata2Hdfs extends MoveOpration 
{
private static Logger log = LoggerFactory.getLogger(Teradata2Hdfs.class);
	
	public Teradata2Hdfs(){}
	
	public Teradata2Hdfs(String type, ConfigDomain configDto)
	{
		super(type, configDto);
	}

	@Override
	public void move() throws BaseException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void valid() throws BaseException {
		// TODO Auto-generated method stub
		
	}
}
