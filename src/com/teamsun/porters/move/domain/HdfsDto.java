package com.teamsun.porters.move.domain;

public class HdfsDto extends BaseMoveDomain
{
	private String hdfsLoc;
	private String fieldsTerminatedBy;
	private String inputNullString;
	private String nullNonString;

	public String getFieldsTerminatedBy() {
		return fieldsTerminatedBy;
	}

	public void setFieldsTerminatedBy(String fieldsTerminatedBy) {
		this.fieldsTerminatedBy = fieldsTerminatedBy;
	}

	public String getInputNullString() {
		return inputNullString;
	}

	public void setInputNullString(String inputNullString) {
		this.inputNullString = inputNullString;
	}

	public String getNullNonString() {
		return nullNonString;
	}

	public void setNullNonString(String nullNonString) {
		this.nullNonString = nullNonString;
	}

	public String getHdfsLoc() {
		return hdfsLoc;
	}

	public void setHdfsLoc(String hdfsLoc) {
		this.hdfsLoc = hdfsLoc;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hdfsLoc == null) ? 0 : hdfsLoc.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;

		HdfsDto other = (HdfsDto) obj;
		
		if (hdfsLoc == null) 
		{
			if (other.hdfsLoc != null)
				return false;
		}
		else if (!hdfsLoc.equals(other.hdfsLoc))
			return false;

		return true;
	}
	
	
}
