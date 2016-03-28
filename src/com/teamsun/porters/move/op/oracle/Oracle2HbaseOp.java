package com.teamsun.porters.move.op.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.exe.MoveMain;
import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HbaseDto;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.domain.table.ColumnsDto;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.factory.MoveDtoFactory;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.util.Constants;
import com.teamsun.porters.move.util.SqoopUtils;
import com.teamsun.porters.move.util.StringUtils;

public class Oracle2HbaseOp extends MoveOpration
{
	private static Logger log = LoggerFactory.getLogger(Oracle2HbaseOp.class);
	
	public Oracle2HbaseOp(){}
	
	public Oracle2HbaseOp(String type, ConfigDomain configDto)
	{
		super(type, configDto);
	}

	@Override
	public void move() throws BaseException 
	{
		BaseMoveDomain srcDto = MoveDtoFactory.createSrcDto(configDto);
		BaseMoveDomain destDto = MoveDtoFactory.createDestDto(configDto);
		
		OracleDto oracleDto = (OracleDto)srcDto;
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
		
		
		if (StringUtils.isEmpty(oracleDto.getQuerySql()) && StringUtils.isEmpty(oracleDto.getColumns()))
		{
			StringBuffer querySql = new StringBuffer("SELECT ");
			for (ColumnsDto colDto : oracleDto.getTableDto().getColumnList())
			{
				querySql.append(colDto.getColumnName() + " AS " + colDto.getColumnName().toUpperCase() + ", ");
			}
			
			querySql.append(rowkeyCol);
			querySql.append(" FROM " + oracleDto.getTableName());
			querySql.append("  WHERE \\$CONDITIONS ");
			
			oracleDto.setQuerySql(querySql.toString());
			
			sqoopCommand = SqoopUtils.genImportFromOralceToHbase(srcDto, destDto, false);
		}
		else
		{
			if (StringUtils.isEmpty(oracleDto.getQuerySql()))
			{
				String columns = "";
				
				String[] colArr = oracleDto.getColumns().split(",");
				
				for (String columnTmp : colArr)
				{
					columns += columnTmp + " AS " + columnTmp.toUpperCase() + ", ";
				}
				
				String querySql = "SELECT " + columns + rowkeyCol + " FROM " + oracleDto.getTableName() + " WHERE \\$CONDITIONS";
				oracleDto.setQuerySql(querySql);
			}
			
			sqoopCommand = SqoopUtils.genImportFromOralceToHbase(oracleDto, destDto, false);
		}
		
		
		log.info("begin to from oracle to hbase");
		String command = sqoopCommand;
		String res = runCommand(command);
		log.info("run command res: " + res);
		log.info("from oracle to hbase finish");
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
		
		if (StringUtils.isEmpty(configDto.getSourceDBTns()))
		{
			if (StringUtils.isEmpty(configDto.getSourceDBIp()))
			{
				throw new BaseException("源数据库IP不能为空");
			}
			if (StringUtils.isEmpty(configDto.getSourceDBName()))
			{
				throw new BaseException("源数据库名不能为空");
			}
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
