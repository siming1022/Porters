package com.teamsun.porters.move.op.teradata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.exe.MoveMain;
import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HbaseDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.domain.table.ColumnsDto;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.factory.MoveDtoFactory;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.util.Constants;
import com.teamsun.porters.move.util.SqoopUtils;
import com.teamsun.porters.move.util.StringUtils;

public class Teradata2Hbase extends MoveOpration 
{
private static Logger log = LoggerFactory.getLogger(Teradata2Hbase.class);
	
	public Teradata2Hbase(){}
	
	public Teradata2Hbase(String type, ConfigDomain configDto)
	{
		super(type, configDto);
	}

	@Override
	public void move() throws BaseException 
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		TeradataDto teradataDto = (TeradataDto)srcDto;
		HbaseDto hbaseDto = (HbaseDto) destDto;
		String sqoopCommand = null;
		
		String[] rowkeyCols = hbaseDto.getRowkeys().split(",");
		String rowkeyCol = "";
		boolean isProtectRowkey = Boolean.parseBoolean(MoveMain.configPro.get(Constants.CONFIG_HBASE_ROWKEY_PROTECTED).toString());
		for (String rowkeyColTmp : rowkeyCols)
		{
			if (isProtectRowkey)
			{
				rowkeyColTmp = "COALESCE(" + rowkeyColTmp + ", '" + rowkeyColTmp + "_NULL')";
			}
			rowkeyCol += rowkeyColTmp + " || '" + Constants.HBASE_ROWKEY_SPLIT + "' || ";
		}
		
		rowkeyCol = rowkeyCol.substring(0, rowkeyCol.length() - 11) + " AS ROWKEY";
		
		if (StringUtils.isEmpty(teradataDto.getQuerySql()) && StringUtils.isEmpty(teradataDto.getColumns()))
		{
			StringBuffer querySql = new StringBuffer("SELECT ");
			for (ColumnsDto colDto : teradataDto.getTableDto().getColumnList())
			{
				querySql.append(colDto.getColumnName() + " AS " + colDto.getColumnName().toUpperCase() + ", ");
			}
			
			querySql.append(rowkeyCol);
			querySql.append(" FROM " + teradataDto.getDatabaseName() + "." + teradataDto.getTableName());
			querySql.append("  WHERE \\$CONDITIONS ");
			
			teradataDto.setQuerySql(querySql.toString());
			
			sqoopCommand = SqoopUtils.genImportFromTeradataToHbase(srcDto, destDto, false);
		}
		else
		{
			if (StringUtils.isEmpty(teradataDto.getQuerySql()))
			{
				String columns = "";
				
				String[] colArr = teradataDto.getColumns().split(",");
				
				for (String columnTmp : colArr)
				{
					columns += columnTmp + " AS " + columnTmp.toUpperCase() + ", ";
				}
				
				String querySql = "SELECT " + columns + rowkeyCol + " FROM " + teradataDto.getDatabaseName() + "." + teradataDto.getTableName() + " WHERE \\$CONDITIONS";
				teradataDto.setQuerySql(querySql);
			}
			
			sqoopCommand = SqoopUtils.genImportFromTeradataToHbase(teradataDto, destDto, false);
		}
		
		
		log.info("begin to from teradata to hbase");
		String command = sqoopCommand;
		String res = runCommand(command);
		log.info("run command res: " + res);
		log.info("from teradata to hbase finish");
	}

	@Override
	public void valid() throws BaseException 
	{
		if (StringUtils.isEmpty(configDto.getSourceTable()))
		{
			throw new BaseException("源表名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestTable()))
		{
			throw new BaseException("目的表名不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestTableRowkey()))
		{
			throw new BaseException("目的表Rowkey不能为空");
		}
		if (StringUtils.isEmpty(configDto.getDestTableColFamily()))
		{
			throw new BaseException("目的表列簇不能为空");
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
	}
}
