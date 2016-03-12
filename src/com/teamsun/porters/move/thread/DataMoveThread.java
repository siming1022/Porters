package com.teamsun.porters.move.thread;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.server.MoveServer;
import com.teamsun.porters.move.util.Constants;

public class DataMoveThread extends Thread 
{
	private static Logger log = LoggerFactory.getLogger(DataMoveThread.class);
	private List<ConfigDomain> configDatas = new ArrayList<ConfigDomain>();
	
	@Resource
	private MoveServer moveServer;
	
	public void setMoveServer(MoveServer moveServer) {
		this.moveServer = moveServer;
	}

	public DataMoveThread() 
	{
		super();
	}

	public DataMoveThread(List<ConfigDomain> configData) 
	{
		this.configDatas = configData;
	}
	
	@Override
	public void run() 
	{
		for (ConfigDomain configDto : this.configDatas)
		{
			try 
			{
				if (Constants.DATA_SOUCE_TYPE_H2H.equals(configDto.getType()))
				{
					HdfsDto srcDto = new HdfsDto();
					HdfsDto destDto = new HdfsDto();
					
					moveServer.move2Hdfs(srcDto, destDto);
				}
				else if(Constants.DATA_SOUCE_TYPE_H2H.equals(configDto.getType()))
				{
					
				}
				else if(Constants.DATA_SOUCE_TYPE_H2H.equals(configDto.getType()))
				{
					
				}
				else if(Constants.DATA_SOUCE_TYPE_H2H.equals(configDto.getType()))
				{
					
				}
				else if(Constants.DATA_SOUCE_TYPE_H2H.equals(configDto.getType()))
				{
					
				}
				else if(Constants.DATA_SOUCE_TYPE_H2H.equals(configDto.getType()))
				{
					
				}
			}
			catch (BaseException e) 
			{
				e.printStackTrace();
			}
		}
	}

}
