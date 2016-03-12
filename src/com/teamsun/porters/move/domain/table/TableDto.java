package com.teamsun.porters.move.domain.table;

import java.util.ArrayList;
import java.util.List;

public class TableDto 
{
	private String tableName; 
    private String tableComment; 
    private List<ColumnsDto> columnList = new ArrayList<ColumnsDto>();
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTableComment() {
		return tableComment;
	}
	public void setTableComment(String tableComment) {
		this.tableComment = tableComment;
	}
	public List<ColumnsDto> getColumnList() {
		return columnList;
	}
	public void setColumnList(List<ColumnsDto> columnList) {
		this.columnList = columnList;
	}
    
    
}
