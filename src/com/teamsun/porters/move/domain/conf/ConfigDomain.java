package com.teamsun.porters.move.domain.conf;

import com.teamsun.porters.move.domain.BaseMoveDomain;

public class ConfigDomain extends BaseMoveDomain
{
	private String type;
	private String dataSource;
	private String dataDest;
	private String sourceTable;
	private String destTable;
	private String querySql;
	private String sourceHdfsLoc;
	private String destHdfsLoc;
	private String sourceDBIp;
	private String destDBIp;
	private String sourceDBPort;
	private String destDBPort;
	private String souceDBName;
	private String destDBName;
	private String souceDBUserName;
	private String destDBUserName;
	private String souceDBPwd;
	private String destDBPwd;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getDataSource() {
		return dataSource;
	}
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}
	public String getDataDest() {
		return dataDest;
	}
	public void setDataDest(String dataDest) {
		this.dataDest = dataDest;
	}
	public String getSourceTable() {
		return sourceTable;
	}
	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}
	public String getDestTable() {
		return destTable;
	}
	public void setDestTable(String destTable) {
		this.destTable = destTable;
	}
	public String getQuerySql() {
		return querySql;
	}
	public void setQuerySql(String querySql) {
		this.querySql = querySql;
	}
	public String getSourceHdfsLoc() {
		return sourceHdfsLoc;
	}
	public void setSourceHdfsLoc(String sourceHdfsLoc) {
		this.sourceHdfsLoc = sourceHdfsLoc;
	}
	public String getDestHdfsLoc() {
		return destHdfsLoc;
	}
	public void setDestHdfsLoc(String destHdfsLoc) {
		this.destHdfsLoc = destHdfsLoc;
	}
	public String getSourceDBIp() {
		return sourceDBIp;
	}
	public void setSourceDBIp(String sourceDBIp) {
		this.sourceDBIp = sourceDBIp;
	}
	public String getDestDBIp() {
		return destDBIp;
	}
	public void setDestDBIp(String destDBIp) {
		this.destDBIp = destDBIp;
	}
	public String getSourceDBPort() {
		return sourceDBPort;
	}
	public void setSourceDBPort(String sourceDBPort) {
		this.sourceDBPort = sourceDBPort;
	}
	public String getDestDBPort() {
		return destDBPort;
	}
	public void setDestDBPort(String destDBPort) {
		this.destDBPort = destDBPort;
	}
	public String getSouceDBName() {
		return souceDBName;
	}
	public void setSouceDBName(String souceDBName) {
		this.souceDBName = souceDBName;
	}
	public String getDestDBName() {
		return destDBName;
	}
	public void setDestDBName(String destDBName) {
		this.destDBName = destDBName;
	}
	public String getSouceDBUserName() {
		return souceDBUserName;
	}
	public void setSouceDBUserName(String souceDBUserName) {
		this.souceDBUserName = souceDBUserName;
	}
	public String getDestDBUserName() {
		return destDBUserName;
	}
	public void setDestDBUserName(String destDBUserName) {
		this.destDBUserName = destDBUserName;
	}
	public String getSouceDBPwd() {
		return souceDBPwd;
	}
	public void setSouceDBPwd(String souceDBPwd) {
		this.souceDBPwd = souceDBPwd;
	}
	public String getDestDBPwd() {
		return destDBPwd;
	}
	public void setDestDBPwd(String destDBPwd) {
		this.destDBPwd = destDBPwd;
	}
}
