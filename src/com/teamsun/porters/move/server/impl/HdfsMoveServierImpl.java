package com.teamsun.porters.move.server.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.server.MoveServer;

/**
 * 
 * @author Administrator
 *
 */
public class HdfsMoveServierImpl implements MoveServer 
{
	private static Logger log = LoggerFactory.getLogger(HdfsMoveServierImpl.class);

	private Configuration conf = new Configuration();
	
	public void initConf()
	{
		try 
		{
			conf.addResource("core-site.xml");
			conf.addResource("hdfs-site.xml");
			conf.addResource("yarn-site.xml");
			conf.addResource("mapred-site.xml");
			conf.addResource("hbase-site.xml");
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void move2Hdfs(BaseMoveDomain srcDomain, BaseMoveDomain destDomain)
			throws BaseException {
		// TODO Auto-generated method stub

	}

	@Override
	public void move2Td(BaseMoveDomain srcDomain, BaseMoveDomain destDomain)
			throws BaseException 
	{
		HdfsDto hdfsDto = (HdfsDto) srcDomain;
		TeradataDto teradataDto = (TeradataDto) destDomain;
		
		String jobName = "FROM HDFS: " + hdfsDto.getHdfsLoc() + " LOAD DATA TO TERADATA: " + teradataDto.getIp() + ", " + teradataDto.getDatabaseName();
		
		runNewJob(jobName, hdfsDto, destDomain);
	}

	@Override
	public void move2Oracle(BaseMoveDomain srcDomain, BaseMoveDomain destDomain)
			throws BaseException {
		// TODO Auto-generated method stub

	}

	@Override
	public void move2Hbase(BaseMoveDomain srcDomain, BaseMoveDomain destDomain)
			throws BaseException {
		// TODO Auto-generated method stub

	}

	@Override
	public void move2Vertica(BaseMoveDomain srcDomain, BaseMoveDomain destDomain)
			throws BaseException {
		// TODO Auto-generated method stub

	}
	
	private void runNewJob(String jobName, BaseMoveDomain srcDto, BaseMoveDomain destDto) throws BaseException
	{
		try {
			HdfsDto hdfsDto = (HdfsDto) srcDto;
			
			conf.set("jobType", "HDFS2ORACLE");
			conf.set("jobDestDto", encode(destDto));
			
			Job job = new Job(conf, jobName);
			
			job.setJarByClass(HdfsMoveServierImpl.class);
			
//		job.setMapperClass(DataMovingMapper.class);
//		job.setCombinerClass(DataMovingReduce.class);
//		job.setReducerClass(DataMovingReduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(
					hdfsDto.getHdfsLoc()));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			throw new BaseException(e.getMessage());
		}
	}
	
	private String encode(BaseMoveDomain dto)
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
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
		
		return str;
	}

}
