package com.teamsun.porters.move.thread;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.server.MoveServer;
import com.teamsun.porters.move.util.Constants;

public class DataMoveThread extends Thread 
{
	private static Logger log = LoggerFactory.getLogger(DataMoveThread.class);
	private List<MoveOpration> moveOps = new ArrayList<MoveOpration>();
	
	@Resource
	private MoveServer moveServer;
	
	public void setMoveServer(MoveServer moveServer) {
		this.moveServer = moveServer;
	}

	public DataMoveThread() 
	{
		super();
	}

	public DataMoveThread(List<MoveOpration> moveOps) 
	{
		this.moveOps = moveOps;
	}
	
	@Override
	public void run() 
	{
		for (MoveOpration moveOp : moveOps)
		{
			try 
			{
				moveOp.move();
			}
			catch (BaseException e) 
			{
				e.printStackTrace();
			}
		}
	}

}
