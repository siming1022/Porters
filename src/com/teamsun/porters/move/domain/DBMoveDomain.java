package com.teamsun.porters.move.domain;

import com.teamsun.porters.move.domain.table.TableDto;


public abstract class DBMoveDomain extends BaseMoveDomain
{
	private String databaseName;
	private String jdbcUrl;
	private String tableName;
	private String driverClass;
	private TableDto tableDto;
	private String querySql;
	private String columns;
	private String tns;
	
	public String getTns() {
		return tns;
	}

	public void setTns(String tns) {
		this.tns = tns;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getQuerySql() {
		return querySql;
	}

	public void setQuerySql(String querySql) {
		this.querySql = querySql;
	}

	public TableDto getTableDto() {
		return tableDto;
	}

	public void setTableDto(TableDto tableDto) {
		this.tableDto = tableDto;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getDriverClass() {
		return driverClass;
	}

	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}
	
	public String getJdbcUrl() {
		return jdbcUrl;
	}
	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
