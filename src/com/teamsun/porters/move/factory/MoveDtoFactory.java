package com.teamsun.porters.move.factory;

import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.domain.HbaseDto;
import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.MySqlDto;
import com.teamsun.porters.move.domain.OracleDto;
import com.teamsun.porters.move.domain.TeradataDto;
import com.teamsun.porters.move.domain.VerticaDto;
import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.util.Constants;
import com.teamsun.porters.move.util.DBMSMetaUtil;
import com.teamsun.porters.move.util.StringUtils;
import com.teamsun.porters.move.util.DBMSMetaUtil.DATABASETYPE;

public class MoveDtoFactory 
{
	public static BaseMoveDomain createSrcDto(ConfigDomain configDto)
	{
		String dataSource = configDto.getDataSource().toUpperCase();
		
		if (Constants.DATA_TYPE_HDFS.equals(dataSource))
		{
			HdfsDto dto = new HdfsDto();
			dto.setHdfsLoc(configDto.getSourceHdfsLoc());
			
			return dto;
		}
		else if (Constants.DATA_TYPE_ORACLE.equals(dataSource))
		{
			OracleDto dto = new OracleDto();
			dto.setDatabaseName(configDto.getSourceDBName());
			dto.setIp(configDto.getSourceDBIp());
			dto.setPort(configDto.getSourceDBPort());
			dto.setUserName(configDto.getSourceDBUserName());
			dto.setPasswd(configDto.getSourceDBPwd());
			dto.setTableName(configDto.getSourceTable());
			dto.setQuerySql(configDto.getQuerySql());
			dto.setTns(configDto.getSourceDBTns().replaceAll(" ", "").replaceAll("\n", ""));
			
			DATABASETYPE databaseType = DBMSMetaUtil.parseDATABASETYPE(dataSource);
			dto.setDriverClass(Constants.DB_DRIVER_CLASS_ORACLE);
			String jdbcUrl = StringUtils.isEmpty(dto.getTns())?DBMSMetaUtil.concatDBURL(databaseType, configDto.getSourceDBIp(), configDto.getSourceDBPort(), configDto.getSourceDBName()):dto.getTns();
			dto.setJdbcUrl(jdbcUrl);
			dto.setTableDto(DBMSMetaUtil.getTableDto(databaseType, Constants.DB_DRIVER_CLASS_ORACLE, configDto.getSourceDBIp(), configDto.getSourceDBPort(), configDto.getSourceDBName()
			, configDto.getSourceDBUserName(), configDto.getSourceDBPwd(), configDto.getSourceTable()));
			
			return dto;
		}
		else if (Constants.DATA_TYPE_TERADATA.equals(dataSource))
		{
			TeradataDto dto = new TeradataDto();
			dto.setDatabaseName(configDto.getSourceDBName());
			dto.setIp(configDto.getSourceDBIp());
			dto.setPort(configDto.getSourceDBPort());
			dto.setUserName(configDto.getSourceDBUserName());
			dto.setPasswd(configDto.getSourceDBPwd());
			dto.setTableName(configDto.getSourceTable());
			dto.setQuerySql(configDto.getQuerySql());
			dto.setColumns(configDto.getColumns());
			
			DATABASETYPE databaseType = DBMSMetaUtil.parseDATABASETYPE(dataSource);
			dto.setDriverClass(Constants.DB_DRIVER_CLASS_TERADATA);
			String jdbcUrl = StringUtils.isEmpty(dto.getTns())?DBMSMetaUtil.concatDBURL(databaseType, configDto.getSourceDBIp(), configDto.getSourceDBPort(), configDto.getSourceDBName()):dto.getTns();
			dto.setJdbcUrl(jdbcUrl);
			dto.setTableDto(DBMSMetaUtil.getTableDto(databaseType, Constants.DB_DRIVER_CLASS_TERADATA, configDto.getSourceDBIp(), configDto.getSourceDBPort(), configDto.getSourceDBName()
			, configDto.getSourceDBUserName(), configDto.getSourceDBPwd(), configDto.getSourceTable()));
			
			return dto;
		}
		else
		{
			return null;
		}
	}

	public static BaseMoveDomain createDestDto(ConfigDomain configDto)
	{
		String dataDest = configDto.getDataDest().toUpperCase();
		
		if (Constants.DATA_TYPE_HDFS.equals(dataDest))
		{
			HdfsDto dto = new HdfsDto();
			dto.setHdfsLoc(configDto.getDestHdfsLoc());
			
			return dto;
		}
		else if (Constants.DATA_TYPE_ORACLE.equals(dataDest))
		{
			OracleDto dto = new OracleDto();
			dto.setDatabaseName(configDto.getDestDBName());
			dto.setIp(configDto.getDestDBIp());
			dto.setPort(configDto.getDestDBPort());
			dto.setUserName(configDto.getDestDBUserName());
			dto.setPasswd(configDto.getDestDBPwd());
			dto.setTableName(configDto.getDestTable());
			dto.setTns(configDto.getDestDBTns().replaceAll(" ", "").replaceAll("\n", ""));
			
			DATABASETYPE databaseType = DBMSMetaUtil.parseDATABASETYPE(dataDest);
			dto.setDriverClass(Constants.DB_DRIVER_CLASS_ORACLE);
			
			String jdbcUrl = StringUtils.isEmpty(dto.getTns())?DBMSMetaUtil.concatDBURL(databaseType, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()):dto.getTns();
			dto.setJdbcUrl(jdbcUrl);
			dto.setTableDto(DBMSMetaUtil.getTableDto(databaseType, Constants.DB_DRIVER_CLASS_ORACLE, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()
			, configDto.getDestDBUserName(), configDto.getDestDBPwd(), configDto.getDestTable()));
			
			return dto;
		}
		else if (Constants.DATA_TYPE_TERADATA.equals(dataDest))
		{
			TeradataDto dto = new TeradataDto();
			
			dto.setDatabaseName(configDto.getDestDBName());
			dto.setIp(configDto.getDestDBIp());
			dto.setPort(configDto.getDestDBPort());
			dto.setUserName(configDto.getDestDBUserName());
			dto.setPasswd(configDto.getDestDBPwd());
			dto.setTableName(configDto.getDestTable());
			
			DATABASETYPE databaseType = DBMSMetaUtil.parseDATABASETYPE(dataDest);
			dto.setDriverClass(Constants.DB_DRIVER_CLASS_TERADATA);
			String jdbcUrl = StringUtils.isEmpty(dto.getTns())?DBMSMetaUtil.concatDBURL(databaseType, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()):dto.getTns();
			dto.setJdbcUrl(jdbcUrl);
			dto.setTableDto(DBMSMetaUtil.getTableDto(databaseType, Constants.DB_DRIVER_CLASS_TERADATA, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()
					, configDto.getDestDBUserName(), configDto.getDestDBPwd(), configDto.getDestTable()));
			
			return dto;
		}
		else if (Constants.DATA_TYPE_VERTICA.equals(dataDest))
		{
			VerticaDto dto = new VerticaDto();
			
			dto.setDatabaseName(configDto.getDestDBName());
			dto.setIp(configDto.getDestDBIp());
			dto.setPort(configDto.getDestDBPort());
			dto.setUserName(configDto.getDestDBUserName());
			dto.setPasswd(configDto.getDestDBPwd());
			dto.setTableName(configDto.getDestTable());
			
			DATABASETYPE databaseType = DBMSMetaUtil.parseDATABASETYPE(dataDest);
			dto.setDriverClass(Constants.DB_DRIVER_CLASS_VERTICA);
			String jdbcUrl = StringUtils.isEmpty(dto.getTns())?DBMSMetaUtil.concatDBURL(databaseType, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()):dto.getTns();
			dto.setJdbcUrl(jdbcUrl);
			dto.setTableDto(DBMSMetaUtil.getTableDto(databaseType, Constants.DB_DRIVER_CLASS_VERTICA, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()
					, configDto.getDestDBUserName(), configDto.getDestDBPwd(), configDto.getDestTable()));
			
			return dto;
		}
		else if (Constants.DATA_TYPE_HBASE.equals(dataDest))
		{
			HbaseDto dto = new HbaseDto();
			
			dto.setDatabaseName(configDto.getDestDBName());
			dto.setIp(configDto.getDestDBIp());
			dto.setPort(configDto.getDestDBPort());
			dto.setUserName(configDto.getDestDBUserName());
			dto.setPasswd(configDto.getDestDBPwd());
			dto.setTableName(configDto.getDestTable());
			dto.setRowkeys(configDto.getDestTableRowkey());
			
			DATABASETYPE databaseType = DBMSMetaUtil.parseDATABASETYPE(dataDest);
			dto.setDriverClass(Constants.DB_DRIVER_CLASS_HIVE);
			String jdbcUrl = StringUtils.isEmpty(dto.getTns())?DBMSMetaUtil.concatDBURL(databaseType, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()):dto.getTns();
			dto.setJdbcUrl(jdbcUrl);
			dto.setTableDto(DBMSMetaUtil.getTableDto(databaseType, Constants.DB_DRIVER_CLASS_HIVE, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()
					, configDto.getDestDBUserName(), configDto.getDestDBPwd(), configDto.getDestTable()));
			
			return dto;
		}
		else if (Constants.DATA_TYPE_MYSQL.equals(dataDest))
		{
			MySqlDto dto = new MySqlDto();
			
			dto.setDatabaseName(configDto.getDestDBName());
			dto.setIp(configDto.getDestDBIp());
			dto.setPort(configDto.getDestDBPort());
			dto.setUserName(configDto.getDestDBUserName());
			dto.setPasswd(configDto.getDestDBPwd());
			dto.setTableName(configDto.getDestTable());
			
			DATABASETYPE databaseType = DBMSMetaUtil.parseDATABASETYPE(dataDest);
			dto.setDriverClass(Constants.DB_DRIVER_CLASS_MYSQL);
			String jdbcUrl = StringUtils.isEmpty(dto.getTns())?DBMSMetaUtil.concatDBURL(databaseType, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()):dto.getTns();
			dto.setJdbcUrl(jdbcUrl);
			dto.setTableDto(DBMSMetaUtil.getTableDto(databaseType, Constants.DB_DRIVER_CLASS_MYSQL, configDto.getDestDBIp(), configDto.getDestDBPort(), configDto.getDestDBName()
					, configDto.getDestDBUserName(), configDto.getDestDBPwd(), configDto.getDestTable()));
			
			return dto;
		}
		else
		{
			return null;
		}
	}
}
