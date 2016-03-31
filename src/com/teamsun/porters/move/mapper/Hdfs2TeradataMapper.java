package com.teamsun.porters.move.mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.table.ColumnsDto;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.util.Constants;
import com.teamsun.porters.move.util.SqlUtils;

public class Hdfs2TeradataMapper extends Mapper<Object, Text, Text, IntWritable> 
{
	private static Logger log = LoggerFactory.getLogger(Hdfs2TeradataMapper.class);
	private int inputRecordCount = 0;
	
	public Hdfs2TeradataMapper() 
	{
		super();
	}
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)	throws IOException, InterruptedException 
	{
		try
		{
//			context.getCounter(org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS).;
			HdfsDto hdfsDto = (HdfsDto) decode(context.getConfiguration().get("srcDto"));
			TeradataDto teradataDto = (TeradataDto) decode(context.getConfiguration().get("destDto"));
			String insertColsSql = context.getConfiguration().get("insertColsSql");
			Statement stm = (Statement) decode(context.getConfiguration().get("stm"));
			
			int colSize = teradataDto.getTableDto().getColumnList().size();
			
			String[] values = value.toString().split(Constants.DATA_SPLIT);
			
			if (values.length < colSize)
			{
				return;
			}
			
			String insertValsSql = "";
			
			for (int i = 0; i < colSize; i++)
			{
				ColumnsDto colDto = teradataDto.getTableDto().getColumnList().get(i);
				insertValsSql += SqlUtils.getValue(colDto.getSqlType(), values[i], Constants.DATA_TYPE_TERADATA) + ", ";
			}
			
			insertValsSql = insertValsSql.substring(0, insertValsSql.length() -2) + ")";
			
			String insertSql = insertColsSql += insertValsSql;
			
			stm.addBatch(insertSql);
			
			inputRecordCount++;
			
			if (inputRecordCount > 0 && inputRecordCount % 500 == 0)
			{
				stm.executeBatch();
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			throw new BaseException(e.getMessage());
		}
	}
	
	private Object decode(String dtoStr)
	{
		Object dto = null;
		ObjectInputStream in = null;
		try 
		{
			dtoStr = URLDecoder.decode(dtoStr, "UTF-8");
			in = new ObjectInputStream(new ByteArrayInputStream(dtoStr.getBytes("ISO-8859-1")));
			
			dto = in.readObject();
			
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try 
			{
				in.close();
			}
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
		
		return dto;
	}


}
