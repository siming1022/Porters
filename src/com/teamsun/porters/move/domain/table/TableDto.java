package com.teamsun.porters.move.domain.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TableDto implements Serializable
{
	private String tableName; 
    private String tableComment; 
    private String partitionCol; 
    private List<ColumnsDto> columnList = new ArrayList<ColumnsDto>();
    private List<String> partitionList = new ArrayList<String>();
    
	public String getPartitionCol() {
		return partitionCol;
	}
	public void setPartitionCol(String partitionCol) {
		this.partitionCol = partitionCol;
	}
	public List<String> getPartitionList() {
		return partitionList;
	}
	public void setPartitionList(List<String> partitionList) {
		this.partitionList = partitionList;
	}
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
