package com.teamsun.porters.move.domain;

import java.io.Serializable;

public abstract class BaseMoveDomain implements Serializable
{
	protected String ip;
	protected String userName;
	protected String passwd;
	protected long port;
	
	public long getPort() {
		return port;
	}
	public void setPort(long port) {
		this.port = port;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPasswd() {
		return passwd;
	}
	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}
	
}
