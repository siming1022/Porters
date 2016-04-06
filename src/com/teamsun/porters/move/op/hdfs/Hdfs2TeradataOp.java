package com.teamsun.porters.move.op.hdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.domain.table.ColumnsDto;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.factory.MoveDtoFactory;
import com.teamsun.porters.move.mapper.Hdfs2TeradataMapper;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.thread.Hdfs2TeradataThread;
import com.teamsun.porters.move.util.Constants;
import com.teamsun.porters.move.util.DBMSMetaUtil;
import com.teamsun.porters.move.util.StringUtils;

public class Hdfs2TeradataOp extends MoveOpration
{
	private static Logger log = LoggerFactory.getLogger(Hdfs2TeradataOp.class);
	
	public Hdfs2TeradataOp(){}
	
	public Hdfs2TeradataOp(String type, ConfigDomain configDto)
	{
		super(type, configDto);
	}
	
	@Override
	public void valid() throws BaseException 
	{
		if (StringUtils.isEmpty(configDto.getSourceHdfsLoc()))
		{
			throw new BaseException("源HDFS地址不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestTable()))
		{
			throw new BaseException("目的表名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestDBIp()))
		{
			throw new BaseException("目的数据库IP不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestDBName()))
		{
			throw new BaseException("目的数据库名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestDBUserName()))
		{
			throw new BaseException("目的数据库用户名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestDBPwd()))
		{
			throw new BaseException("目的数据库密码不能为空");
		}
	}

	public void move2() throws BaseException 
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		HdfsDto hdfsDto = (HdfsDto) srcDto;
		TeradataDto teradataDto = (TeradataDto) destDto;
		
		try 
		{
			Configuration config = getConfig(null);
			config.set("srcDto", encode(srcDto));
			config.set("destDto", encode(destDto));
			config.set("type", type);
			config.set("insertColsSql", getInsertSql(teradataDto));
			
			
			Job job = getJob(config, type);
			
			job.setJarByClass(Hdfs2TeradataOp.class);
			job.setMapperClass(Hdfs2TeradataMapper.class);
//    		job.setCombinerClass(DataMovingReduce.class);
//   	 	job.setReducerClass(DataMovingReduce.class);
			 
			job.setNumReduceTasks(0);
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(IntWritable.class);
			
			// 判断output文件夹是否存在，如果存在则删除  
			Path path = new Path(Constants.OUT_PATH_TEMP);  
			FileSystem fileSystem = path.getFileSystem(config);// 根据path找到这个文件  
			if (fileSystem.exists(path)) 
			{  
			    fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
			}  
			 
			FileInputFormat.addInputPath(job, new Path(hdfsDto.getHdfsLoc()));
			FileOutputFormat.setOutputPath(job, new Path(Constants.OUT_PATH_TEMP));
			
			log.info("begin to from hdfs to teradata");
			runMapReduce(job);
			log.info("from hdfs to teradata finish");
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			throw new BaseException(e.getMessage());
		}
	}
	
	private String getInsertSql(TeradataDto teradataDto) 
	{
		StringBuffer sql = new StringBuffer("INSERT INTO " + teradataDto.getDatabaseName() + "." + teradataDto.getTableName() + " (");
		for (ColumnsDto colDto : teradataDto.getTableDto().getColumnList())
		{
			sql.append(colDto.getColumnName() + ", ");
		}
		
		sql = new StringBuffer(sql.substring(0, sql.length() - 2) + ") VALUES (");
		
		return sql.toString();
	}
	
	@Override
	public void move() throws BaseException
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		HdfsDto hdfsDto = (HdfsDto) srcDto;
		TeradataDto teradataDto = (TeradataDto) destDto;
		
		try 
		{
			
			BlockingQueue<String> queue = new LinkedBlockingQueue<String>(50000);
			
			Configuration config = getConfig(null);
			FileSystem fs = FileSystem.get(config);
			Path path = new Path(hdfsDto.getHdfsLoc());
			
			ExecutorService sendDataPool = Executors.newFixedThreadPool(8);
			
			for (int i = 0; i < 8; i++) 
			{
				sendDataPool.submit(new Hdfs2TeradataThread(queue, teradataDto));
			}
			
			sendDataPool.shutdown();

			readHdfsFile(fs, path, queue);
			
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	private void readHdfsFile(FileSystem fs, Path path, BlockingQueue<String> queue)
	{
		try 
		{
			FileStatus[] fileStatus = fs.listStatus(path);
			
			for (FileStatus fileState : fileStatus)
			{
				if (fileState.isDir())
				{
					readHdfsFile(fs, fileState.getPath(), queue);
				}
				else
				{
					InputStream is = null;
					InputStreamReader isr = null;
					BufferedReader br = null;
					try 
					{
						is = fs.open(fileState.getPath());
						isr = new InputStreamReader(is, "GBK");
						br = new BufferedReader(isr, 5*1024*1024);
						String line = "";
						while((line = br.readLine()) != null) 
						{
							queue.put(line);
						}
					} 
					catch (Exception e) 
					{
						e.printStackTrace();
					}
					finally
					{
						if (br != null)
							br.close();
						if (isr != null)
							isr.close();
						if (is != null)
							is.close();
					}
				}
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	/*public void move() throws BaseException 
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		String sqoopCommand = SqoopUtils.genExportFromHdfsToTeradata(srcDto, destDto);
		
		log.info("begin to from hdfs to teradata");
		String command = sqoopCommand;
		String res = runCommand(command);
		log.info("run command res: " + res);
		log.info("from hdfs to teradata finish");
	}*/
}
