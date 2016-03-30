package com.teamsun.porters.move.domain.table;

import java.io.Serializable;

public class ColumnsDto implements Serializable
{
	private String columnName; 
    private String columnComment; 
    private String SqlType;
    private int columnSize;
    
	public int getColumnSize() {
		return columnSize;
	}
	public void setColumnSize(int columnSize) {
		this.columnSize = columnSize;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getColumnComment() {
		return columnComment;
	}
	public void setColumnComment(String columnComment) {
		this.columnComment = columnComment;
	}
	public String getSqlType() {
		return SqlType;
	}
	public void setSqlType(String sqlType) {
		SqlType = sqlType;
	}
    
    
}
