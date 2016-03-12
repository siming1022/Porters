package com.teamsun.porters.move.domain.table;

public class ColumnsDto 
{
	private String columnName; 
    private String columnComment; 
    private String SqlType;
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
