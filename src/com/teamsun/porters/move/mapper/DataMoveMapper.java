package com.teamsun.porters.move.mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;

public class DataMoveMapper extends Mapper<Object, Text, Text, IntWritable> 
{
	private static Logger log = LoggerFactory.getLogger(DataMoveMapper.class);
	
	public DataMoveMapper() 
	{
		super();
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)	throws IOException, InterruptedException 
	{
		String destDtoStr = context.getConfiguration().get("jobDestDto");
		BaseMoveDomain destDto = decode(destDtoStr);
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) 
		{
//			this.word.set(itr.nextToken());
//			context.write(this.word, one);
		}
	}
	
	private BaseMoveDomain decode(String dtoStr)
	{
		BaseMoveDomain dto = null;
		ObjectInputStream in = null;
		try 
		{
			dtoStr = URLDecoder.decode(dtoStr, "UTF-8");
			in = new ObjectInputStream(new ByteArrayInputStream(dtoStr.getBytes("ISO-8859-1")));
			
			dto = (BaseMoveDomain) in.readObject();
			
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
