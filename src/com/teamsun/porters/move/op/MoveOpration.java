package com.teamsun.porters.move.op;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URLEncoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.mortbay.log.Log;

import com.teamsun.porters.exe.MoveMain;
import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.mapper.Hdfs2TeradataMapper;
import com.teamsun.porters.move.util.StringUtils;

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
//		Log.info(command);
		/*BufferedReader reader = null;
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
		}*/
	    
	    return sb.toString();
	}
	
	public Configuration getConfig(String path)
	{
		Configuration config = new Configuration();
		if (StringUtils.isEmpty(path))
		{
			config.addResource("core-site.xml");
			config.addResource("hdfs-site.xml");
			config.addResource("yarn-site.xml");
			config.addResource("mapred-site.xml");
			config.addResource("hbase-site.xml");
		}
		else
		{
			config.addResource(path + File.separator + "core-site.xml");
			config.addResource(path + File.separator + "hdfs-site.xml");
			config.addResource(path + File.separator + "yarn-site.xml");
			config.addResource(path + File.separator + "mapred-site.xml");
			config.addResource(path + File.separator + "hbase-site.xml");
		}
		
		return config;
	}
	
	public Job getJob(Configuration config, String type) throws BaseException
	{
		Job job = null;
		try 
		{
			 job = new Job(config, type);
		}
		catch (IOException e) 
		{
			e.printStackTrace();
			throw new BaseException(e.getMessage());
		}
		
		return job;
	}
	
	
	public void runMapReduce(Job job) throws BaseException
	{
		try 
		{
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			throw new BaseException(e.getMessage());
		}
	}
	
	public String encode(Object dto)
	{
		String str = "";
		ByteArrayOutputStream baos = null;
		ObjectOutputStream oos = null;
		
		try 
		{
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(dto);
			oos.flush();
			
			str = baos.toString("ISO-8859-1");
			str = URLEncoder.encode(str, "UTF-8");
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try 
			{
				oos.close();
				baos.close();
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
		
		return str;
	}
	
}
