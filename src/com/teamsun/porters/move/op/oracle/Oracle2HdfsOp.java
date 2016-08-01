package com.teamsun.porters.move.op.oracle;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.domain.table.ColumnsDto;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.factory.MoveDtoFactory;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.util.SqoopUtils;
import com.teamsun.porters.move.util.StringUtils;

public class Oracle2HdfsOp extends MoveOpration
{
	private static Logger log = LoggerFactory.getLogger(Oracle2HdfsOp.class);
	
	public Oracle2HdfsOp(){}
	
	public Oracle2HdfsOp(String type, ConfigDomain configDto)
	{
		super(type, configDto);
	}

	@Override
	public void move() throws BaseException 
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		OracleDto oracleDto = (OracleDto)srcDto;
		
		String querySql = null;
		List<String> sqoopCommands = new ArrayList<String>();
		if (StringUtils.isEmpty(oracleDto.getQuerySql()) && StringUtils.isEmpty(oracleDto.getColumns()))
		{
			//如果有分区，则按分区导数据
			if (!StringUtils.isEmptyWithTrim(oracleDto.getTableDto().getPartitionCol()))
			{
				HdfsDto hdfsDto = (HdfsDto) destDto;
				String hdfsLocStr = hdfsDto.getHdfsLoc();
				for (String partition : oracleDto.getTableDto().getPartitionList())
				{
					querySql = getQuerySql(oracleDto, partition);
					oracleDto.setQuerySql(querySql);
					
					hdfsDto.setHdfsLoc(hdfsLocStr + "/" + partition.replaceAll("-", ""));
					sqoopCommands.add(SqoopUtils.genImportFromOralceToHdfs(oracleDto, destDto, false));
				}
			}
			else
			{
				querySql = getQuerySql(oracleDto, null);
				oracleDto.setQuerySql(querySql);
				sqoopCommands.add(SqoopUtils.genImportFromOralceToHdfs(oracleDto, destDto, false));
			}
//			sqoopCommand = SqoopUtils.genImportFromOralceToHdfs(srcDto, destDto, true);
		}
		else if (StringUtils.isEmpty(oracleDto.getQuerySql()))
		{
			querySql = "SELECT " + oracleDto.getColumns() + " FROM " + oracleDto.getTableName() + " WHERE \\$CONDITIONS";
			oracleDto.setQuerySql(querySql);
			sqoopCommands.add(SqoopUtils.genImportFromOralceToHdfs(oracleDto, destDto, false));
		}
		
		
//		log.info("begin to from oracle to hdfs");
		for (String sqoopCommand : sqoopCommands)
		{
			String command = sqoopCommand;
			String res = runCommand(command);
//			log.info("run command res: " + res);
		}
//		log.info("from oracle to hdfs finish");
	}

	private String getQuerySql(OracleDto oracleDto, String partition)
	{
		StringBuffer sb = new StringBuffer("SELECT ");
		for (ColumnsDto cd : oracleDto.getTableDto().getColumnList())
		{
			String colName = cd.getColumnName();
			String colType = cd.getSqlType();
			
			if ("varchar2".equals(colType.toLowerCase()))
				sb.append(" replace(replace(replace(to_char(" + colName + "),chr(10),''),chr(13),''),chr(9),'') as " + colName + ", ");
			else
				sb.append(colName + " as " + colName + ", ");
				
		}
		
		sb = new StringBuffer(sb.substring(0, sb.length() - 2));
		sb.append((" FROM " + oracleDto.getTableName() + " WHERE ") + (partition!=null?(oracleDto.getTableDto().getPartitionCol() + " = '" + partition + "' AND "):"")  + " \\$CONDITIONS");
		
		return sb.toString();
	}

	@Override
	public void valid() throws BaseException 
	{
		if (StringUtils.isEmpty(configDto.getSourceTable()))
		{
			throw new BaseException("源表名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getSourceDBIp()))
		{
			throw new BaseException("源数据库IP不能为空");
		}
		if (StringUtils.isEmpty(configDto.getSourceDBName()))
		{
			throw new BaseException("源数据库名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getSourceDBUserName()))
		{
			throw new BaseException("源数据库用户名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getSourceDBPwd()))
		{
			throw new BaseException("源数据库密码不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestHdfsLoc()))
		{
			throw new BaseException("目的HDFS地址不能为空");
		}
	}
}
