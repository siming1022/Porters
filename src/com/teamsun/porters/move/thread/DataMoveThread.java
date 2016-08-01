package com.teamsun.porters.move.thread;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.op.MoveOpration;

public class DataMoveThread extends Thread 
{
	private static Logger log = LoggerFactory.getLogger(DataMoveThread.class);
	private List<MoveOpration> moveOps = new ArrayList<MoveOpration>();
	private boolean isFinish = false;
	
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
		
		isFinish = true;
		log.info(this.getName() + " run finished");
	}
	
	public boolean isFinished()
	{
		return isFinish;
	}

}
