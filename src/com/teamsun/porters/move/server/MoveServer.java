package com.teamsun.porters.move.server;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.exception.BaseException;

public interface MoveServer 
{
	public void move2Hdfs(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException;
	
	public void move2Td(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException;
	
	public void move2Oracle(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException;

	public void move2Hbase(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException;

	public void move2Vertica(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException;
}
