package com.teamsun.porters.move.domain;

public class HbaseDto extends DBMoveDomain
{
	private String rowkeys;
	private String colFamily;
	
	public String getColFamily() {
		return colFamily;
	}

	public void setColFamily(String colFamily) {
		this.colFamily = colFamily;
	}

	public String getRowkeys() {
		return rowkeys;
	}

	public void setRowkeys(String rowkeys) {
		this.rowkeys = rowkeys;
	}
	
}
