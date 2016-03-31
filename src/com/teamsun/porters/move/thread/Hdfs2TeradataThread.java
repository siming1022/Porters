package com.teamsun.porters.move.thread;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.table.ColumnsDto;
import com.teamsun.porters.move.op.hdfs.Hdfs2TeradataOp;
import com.teamsun.porters.move.util.Constants;
import com.teamsun.porters.move.util.DBMSMetaUtil;
import com.teamsun.porters.move.util.SqlUtils;

public class Hdfs2TeradataThread extends Thread 
{
	private static Logger log = LoggerFactory.getLogger(Hdfs2TeradataOp.class);
	private BlockingQueue<String> queue;
	private BaseMoveDomain baseMoveDomain;
	
	public Hdfs2TeradataThread()
	{
		super();
	}

	public Hdfs2TeradataThread(BlockingQueue<String> queue, BaseMoveDomain baseMoveDomain)
	{
		this.queue = queue;
		this.baseMoveDomain = baseMoveDomain;
	}
	
	
	@Override
	public void run() 
	{
		TeradataDto teradataDto = (TeradataDto) this.baseMoveDomain;
		Connection conn = null;
		Statement stm = null;
		int count = 0;
		int sleepCount = 0;
		try 
		{
			conn = DBMSMetaUtil.getConnection(teradataDto.getDriverClass(), teradataDto.getJdbcUrl(), teradataDto.getUserName(), teradataDto.getPasswd());
			conn.setAutoCommit(false);
			
			stm = conn.createStatement();
			
			String insertColsSql = getInsertSql(teradataDto);
			int colSize = teradataDto.getTableDto().getColumnList().size();
			
			while (true)
			{
				if (sleepCount >= 3)
				{
					stm.executeBatch();
					conn.commit();
					break;
				}
				
				if (queue.size() == 0)
				{
					Thread.sleep(1000);
					sleepCount ++;
					continue;
				}
				
				String[] values = queue.take().split(Constants.DATA_SPLIT);
				
				if (values.length < colSize)
				{
					continue;
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
				count++;
				
				if (count > 0 && count%500==0)
				{
					stm.executeBatch();
					conn.commit();
				}
			}
			
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
			DBMSMetaUtil.close(stm);
			DBMSMetaUtil.close(conn);
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
	
}
