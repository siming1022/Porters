package com.teamsun.porters.move.op;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;

public abstract class MoveOpration
{
	protected ConfigDomain configDto;
	protected String type;
	
	public MoveOpration(){}
	
	public MoveOpration(String type, ConfigDomain configDto)
	{
		this.type = type;
		this.configDto = configDto;
	}
	
	public ConfigDomain getConfigDto() {
		return configDto;
	}

	public void setConfigDto(ConfigDomain configDto) {
		this.configDto = configDto;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	public abstract void move() throws BaseException;

	public abstract void valid() throws BaseException;
	
	public String runCommand(String command) throws BaseException
	{
		StringBuffer sb = new StringBuffer();
		BufferedReader reader = null;
		InputStreamReader isr = null;
		try 
		{
			Process p = Runtime.getRuntime().exec(command);
			p.waitFor();

			isr = new InputStreamReader(p.getInputStream());
			reader = new BufferedReader(isr);

			String line = "";
			
			while ((line = reader.readLine())!= null) 
			{
				sb.append(line + "\n");
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			throw new BaseException(e.getMessage());
		}
		finally
		{
			try 
			{
				if (reader != null)
					reader.close();

				if (isr != null)
					isr.close();
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
	    
	    return sb.toString();
	}
	
	
}
