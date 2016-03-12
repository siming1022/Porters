package com.teamsun.porters.move.valid;

import java.util.Map;

import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.util.StringUtils;

public class ValidConfig 
{
	public static void validH2M(Map<String, Object> config) throws BaseException 
	{
		if (StringUtils.isObjEmpty(config.get("sourceHdfsLoc")))
		{
			throw new BaseException("源HDFS地址不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destTable")))
		{
			throw new BaseException("目的表名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBIp")))
		{
			throw new BaseException("目的数据库IP不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBName")))
		{
			throw new BaseException("目的数据库名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBUserName")))
		{
			throw new BaseException("目的数据库用户名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBPwd")))
		{
			throw new BaseException("目的数据库密码不能为空");
		}
	}

	public static void validH2V(Map<String, Object> config) throws BaseException 
	{
		if (StringUtils.isObjEmpty(config.get("sourceHdfsLoc")))
		{
			throw new BaseException("源HDFS地址不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destTable")))
		{
			throw new BaseException("目的表名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBIp")))
		{
			throw new BaseException("目的数据库IP不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBName")))
		{
			throw new BaseException("目的数据库名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBUserName")))
		{
			throw new BaseException("目的数据库用户名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBPwd")))
		{
			throw new BaseException("目的数据库密码不能为空");
		}
	}

	public static void validH2HB(Map<String, Object> config) throws BaseException 
	{
		if (StringUtils.isObjEmpty(config.get("sourceHdfsLoc")))
		{
			throw new BaseException("源HDFS地址不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destTable")))
		{
			throw new BaseException("目的表名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBIp")))
		{
			throw new BaseException("目的数据库IP不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBName")))
		{
			throw new BaseException("目的数据库名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBUserName")))
		{
			throw new BaseException("目的数据库用户名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBPwd")))
		{
			throw new BaseException("目的数据库密码不能为空");
		}		
	}

	public static void validH2T(Map<String, Object> config) throws BaseException 
	{
		if (StringUtils.isObjEmpty(config.get("sourceHdfsLoc")))
		{
			throw new BaseException("源HDFS地址不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destTable")))
		{
			throw new BaseException("目的表名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBIp")))
		{
			throw new BaseException("目的数据库IP不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBName")))
		{
			throw new BaseException("目的数据库名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBUserName")))
		{
			throw new BaseException("目的数据库用户名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBPwd")))
		{
			throw new BaseException("目的数据库密码不能为空");
		}
	}

	public static void validH2O(Map<String, Object> config) throws BaseException 
	{
		if (StringUtils.isObjEmpty(config.get("sourceHdfsLoc")))
		{
			throw new BaseException("源HDFS地址不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destTable")))
		{
			throw new BaseException("目的表名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBIp")))
		{
			throw new BaseException("目的数据库IP不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBName")))
		{
			throw new BaseException("目的数据库名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBUserName")))
		{
			throw new BaseException("目的数据库用户名不能为空");
		}
		if (StringUtils.isObjEmpty(config.get("destDBPwd")))
		{
			throw new BaseException("目的数据库密码不能为空");
		}
		
	}

	public static void validH2H(Map<String, Object> config) throws BaseException 
	{
		if (StringUtils.isObjEmpty(config.get("sourceHdfsLoc")))
		{
			throw new BaseException("源HDFS地址不能为空");
		}

		if (StringUtils.isObjEmpty(config.get("destHdfsLoc")))
		{
			throw new BaseException("目的HDFS地址不能为空");
		}
		
		String sourceHdfsLoc = config.get("sourceHdfsLoc").toString();
		String destHdfsLoc = config.get("destHdfsLoc").toString();
		
		if (sourceHdfsLoc.equals(destHdfsLoc))
		{
			throw new BaseException("源HDFS地址与目的HDFS地址不能一致");
		}
	}
}
