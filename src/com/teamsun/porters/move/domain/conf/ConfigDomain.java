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
	private String sourceDBName;
	private String destDBName;
	private String sourceDBUserName;
	private String destDBUserName;
	private String sourceDBPwd;
	private String destDBPwd;
	private String destTableRowkey;
	private String columns;
	private String sourceDBTns;
	private String destDBTns;
	
	public String getSourceDBTns() {
		return sourceDBTns;
	}
	public void setSourceDBTns(String sourceDBTns) {
		this.sourceDBTns = sourceDBTns;
	}
	public String getDestDBTns() {
		return destDBTns;
	}
	public void setDestDBTns(String destDBTns) {
		this.destDBTns = destDBTns;
	}
	public String getColumns() {
		return columns;
	}
	public void setColumns(String columns) {
		this.columns = columns;
	}
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
	public String getSourceDBName() {
		return sourceDBName;
	}
	public void setSourceDBName(String sourceDBName) {
		this.sourceDBName = sourceDBName;
	}
	public String getDestDBName() {
		return destDBName;
	}
	public void setDestDBName(String destDBName) {
		this.destDBName = destDBName;
	}
	public String getSourceDBUserName() {
		return sourceDBUserName;
	}
	public void setSourceDBUserName(String sourceDBUserName) {
		this.sourceDBUserName = sourceDBUserName;
	}
	public String getDestDBUserName() {
		return destDBUserName;
	}
	public void setDestDBUserName(String destDBUserName) {
		this.destDBUserName = destDBUserName;
	}
	public String getSourceDBPwd() {
		return sourceDBPwd;
	}
	public void setSourceDBPwd(String sourceDBPwd) {
		this.sourceDBPwd = sourceDBPwd;
	}
	public String getDestDBPwd() {
		return destDBPwd;
	}
	public void setDestDBPwd(String destDBPwd) {
		this.destDBPwd = destDBPwd;
	}
	public String getDestTableRowkey() {
		return destTableRowkey;
	}
	public void setDestTableRowkey(String destTableRowkey) {
		this.destTableRowkey = destTableRowkey;
	}
	
}
