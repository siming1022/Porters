package com.teamsun.porters.move.domain;

import com.teamsun.porters.move.domain.table.TableDto;


public abstract class DBMoveDomain extends BaseMoveDomain
{
	private String databaseName;
	private String jdbcUrl;
	private String tableName;
	private String driverClass;
	private TableDto tableDto;
	
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
