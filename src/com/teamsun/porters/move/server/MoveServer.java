package com.teamsun.porters.move.server;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.exception.BaseException;

public interface MoveServer 
{
	public void move(BaseMoveDomain srcDomain, BaseMoveDomain destDomain) throws BaseException;
}
