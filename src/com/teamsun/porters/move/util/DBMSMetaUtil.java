package com.teamsun.porters.move.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.teamsun.porters.move.connpool.ConnectionPoolManager;
import com.teamsun.porters.move.connpool.bean.DBbean;
import com.teamsun.porters.move.domain.table.ColumnsDto;
import com.teamsun.porters.move.domain.table.TableDto;
import com.teamsun.porters.move.template.SqlTemplate;

/**
 * 需要注意,想要有表字段描述信息，获取连接时需要指定某些特殊属性<br/> 
 * 数据交换-工具类
 */
public class DBMSMetaUtil 
{
	/**
	 * 数据库类型,枚举
	 * 
	 */
	public static enum DATABASETYPE {
		ORACLE, MYSQL, SQLSERVER, SQLSERVER2005, DB2, INFORMIX, SYBASE, TERADATA, VERTICA, HIVE, HBASE, OTHER, EMPTY
	}

	/**
	 * 根据字符串,判断数据库类型
	 * 
	 * @param databasetype
	 * @return
	 */
	public static DATABASETYPE parseDATABASETYPE(String databasetype) {
		// 空类型
		if (null == databasetype || databasetype.trim().length() < 1) {
			return DATABASETYPE.EMPTY;
		}
		// 截断首尾空格,转换为大写
		databasetype = databasetype.trim().toUpperCase();
		// Oracle数据库
		if (databasetype.contains("ORACLE")) {
			//
			return DATABASETYPE.ORACLE;
		}
		// MYSQL 数据库
		if (databasetype.contains("MYSQL")) {
			//
			return DATABASETYPE.MYSQL;
		}
		// SQL SERVER 数据库
		if (databasetype.contains("SQL") && databasetype.contains("SERVER")) {
			//
			if (databasetype.contains("2005") || databasetype.contains("2008") || databasetype.contains("2012")) {

        		try {
					Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				
				return DATABASETYPE.SQLSERVER2005;
			} else {
				try {
					// 注册 JTDS
					Class.forName("net.sourceforge.jtds.jdbc.Driver");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				return DATABASETYPE.SQLSERVER;
			}
		}
		// 下面的这几个没有经过实践测试, 判断可能不准确
		// DB2 数据库
		if (databasetype.contains("DB2")) {
			//
			return DATABASETYPE.DB2;
		}
		// INFORMIX 数据库
		if (databasetype.contains("INFORMIX")) {
			//
			return DATABASETYPE.INFORMIX;
		}
		// SYBASE 数据库
		if (databasetype.contains("SYBASE")) {
			//
			return DATABASETYPE.SYBASE;
		}

		// TERADATA 数据库
		if (databasetype.contains("TERADATA")) {
			//
			return DATABASETYPE.TERADATA;
		}

		// VERTICA 数据库
		if (databasetype.contains("VERTICA")) {
			//
			return DATABASETYPE.VERTICA;
		}

		// HIVE 数据库
		if (databasetype.contains("HIVE") || databasetype.contains("HBASE")) {
			//
			return DATABASETYPE.HIVE;
		}

		// 默认,返回其他
		return DATABASETYPE.OTHER;
	}
	
	public static TableDto getTableDto(DATABASETYPE dbtype, String driverClass, String ip, long port, String dbname, String username, String password, String tableName, long days, String tns)
	{
		TableDto tableDto = new TableDto();
		
		// 去除首尾空格
		ip = trim(ip);
		dbname = trim(dbname);
		username = trim(username);
		password = trim(password);
		tableName = trim(tableName);
		//
		String url = !StringUtils.isEmpty(tns)?tns:concatDBURL(dbtype, ip, port, dbname);
		Connection conn = null;
		ResultSet rs = null;
		//
		try {
			conn = getConnection(driverClass, url, username, password);
			// 这句话我也不懂是什么意思... 好像没什么用
			conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			// 获取Meta信息对象
			DatabaseMetaData meta = conn.getMetaData();
			// 数据库
			String catalog = null;
			// 数据库的用户
			String schemaPattern =null;// meta.getUserName();
			// 表名
			String tableNamePattern = tableName;//
			String types = null;
			// Oracle
			if (DATABASETYPE.ORACLE.equals(dbtype)) {
				schemaPattern = username;
				if (null != schemaPattern) {
					schemaPattern = schemaPattern.toUpperCase();
				}
				// 查询
				rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, types);
			} else if (DATABASETYPE.MYSQL.equals(dbtype)) {
				// Mysql查询
				// MySQL 的 table 这一级别查询不到备注信息
				schemaPattern = dbname;
				rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, types);
			}  else if (DATABASETYPE.SQLSERVER.equals(dbtype) || DATABASETYPE.SQLSERVER2005.equals(dbtype)) {
				// SqlServer
				tableNamePattern = "%";
				rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, types);
			}  else if (DATABASETYPE.DB2.equals(dbtype)) {
				// DB2查询
				schemaPattern = "jence_user";
				tableNamePattern = "%";
				rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, types);
			} else if (DATABASETYPE.INFORMIX.equals(dbtype)) {
				// SqlServer
				tableNamePattern = "%";
				rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, types);
			} else if (DATABASETYPE.SYBASE.equals(dbtype)) {
				// SqlServer
				tableNamePattern = "%";
				rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, types);
			} else if (DATABASETYPE.TERADATA.equals(dbtype)) {
				schemaPattern = dbname;
				rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, types);
			} else if (DATABASETYPE.VERTICA.equals(dbtype)) {
				// SqlServer
				tableNamePattern = "%";
				rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, types);
			} else if (DATABASETYPE.HIVE.equals(dbtype) || DATABASETYPE.HBASE.equals(dbtype)) {
				// SqlServer
				schemaPattern = StringUtils.getValue(dbname, Constants.HIVE_EXT_DATABASE_NAME_PDATA);
				rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, types);
			}  else {
				throw new RuntimeException("不认识的数据库类型!");
			}
			//
			tableDto.setTableName(tableName);
			tableDto = getPartitions(dbtype, parseResulDto(rs, tableDto), conn, days);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close(rs);
			close(conn, url);
		}
		//
		
		if (tableDto == null)
			System.out.println("A");
		return tableDto;
	}
		

	private static TableDto getPartitions(DATABASETYPE dbtype, TableDto tableDto, Connection conn, long days) 
	{
		if (DATABASETYPE.ORACLE.equals(dbtype))
		{
			String queryPartitionsSql = MessageFormat.format(SqlTemplate.ORACLE_QUERY_TABLE_PARTITION_COL_SQL, tableDto.getTableName());
			ResultSet rs = null;
			String sql = null;
			try 
			{
				Statement stm = conn.createStatement();
				
//				System.out.println(queryPartitionsSql);
				rs = stm.executeQuery(queryPartitionsSql);
				while (rs.next())
				{
					String colName = rs.getString("COLUMN_NAME");
					tableDto.setPartitionCol(colName);
					
					
					
					
					//
					
					/*
					Statement stm2 = conn.createStatement();
					if (!"-1".equals(days))
					{
						String partitionWhere = colName + " >= SYSDATE - " + days;
						
						for (ColumnsDto cd : tableDto.getColumnList())
						{
							if (cd.getColumnName().toUpperCase().equals(colName.toUpperCase()))
							{
								//如果分区字段类型不是DATE，就需要将WHERE里的条件改变（哪个B建的表，分区字段干毛要弄别的类型，有猫饼）
								if (!"DATE".equals(cd.getSqlType().toUpperCase()))
								{
									partitionWhere = colName + " >= TO_NUMBER(TO_CHAR(SYSDATE - " + days + ", 'YYYYMMDD'))";
								}
							}
						}
						sql = MessageFormat.format(SqlTemplate.ORACLE_QUERY_TABLE_PARTITION_SQL, colName, tableDto.getTableName(), partitionWhere, colName, colName);
						
					}
					else
					{
						sql = MessageFormat.format(SqlTemplate.ORACLE_QUERY_TABLE_PARTITION_NO_TIME_SQL, colName, tableDto.getTableName(), colName, colName);
					}
					
					
					ResultSet rs2 = stm2.executeQuery(sql);
					List<String> values = new ArrayList<String>();
					while (rs2.next())
					{
						values.add(rs2.getString("value"));
					}*/
					
					boolean isNumber = false;
					for (ColumnsDto cd : tableDto.getColumnList())
					{
						if (cd.getColumnName().toUpperCase().equals(colName.toUpperCase()))
						{
							//如果分区字段类型不是DATE，就需要将WHERE里的条件改变（哪个B建的表，分区字段干毛要弄别的类型，有猫饼）
							if (!"DATE".equals(cd.getSqlType().toUpperCase()))
							{
								isNumber = true;
								break;
							}
						}
					}
					List<String> values = new ArrayList<String>();
					
					Calendar c = Calendar.getInstance();
		    		c.set(Calendar.YEAR, 2015);
		    		c.set(Calendar.MONTH, 00);
		    		c.set(Calendar.DAY_OF_MONTH, 01);
		    		
		    		Calendar c2 = Calendar.getInstance();
		    		c2.set(Calendar.YEAR, 2016);
		    		c2.set(Calendar.MONTH, 7);
		    		c2.set(Calendar.DAY_OF_MONTH, 02);
		    		
		    		while (c2.after(c))
		    		{
		    			String value = "";
		    			if (!isNumber)
		            	{
		    				value = c.get(Calendar.YEAR) + "-" + ((c.get(Calendar.MONTH) + 1) < 10?"0"+(c.get(Calendar.MONTH) + 1):(c.get(Calendar.MONTH) + 1)) + "-" +  ((c.get(Calendar.DAY_OF_MONTH)<10?"0"+c.get(Calendar.DAY_OF_MONTH):c.get(Calendar.DAY_OF_MONTH)));
		            	}
		            	else
		            	{
		            		value = c.get(Calendar.YEAR) + "" + ((c.get(Calendar.MONTH) + 1) < 10?"0"+(c.get(Calendar.MONTH) + 1):(c.get(Calendar.MONTH) + 1)) + "" +  ((c.get(Calendar.DAY_OF_MONTH)<10?"0"+c.get(Calendar.DAY_OF_MONTH):c.get(Calendar.DAY_OF_MONTH)));
		            	}
		    			
		    			values.add(value);
		    			c.add(Calendar.DAY_OF_YEAR, 1);
		    		}
					tableDto.setPartitionList(values);
				}
			}
			catch (SQLException e) 
			{
				e.printStackTrace();
				System.out.println(sql);
			}
		}
		
		
		return tableDto;
	}

	/**
	 * 列出数据库的所有表
	 */
	public static List<Map<String, Object>> listTables(String databasetype, String driverClass, String ip, long port, String dbname,
			String username, String password) {
		// 去除首尾空格
		databasetype = trim(databasetype);
		ip = trim(ip);
		dbname = trim(dbname);
		username = trim(username);
		password = trim(password);
		//
		DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
		//
		List<Map<String, Object>> result = null;
		String url = concatDBURL(dbtype, ip, port, dbname);
		Connection conn = getConnection(driverClass, url, username, password);
		// Statement stmt = null;
		ResultSet rs = null;
		//
		try {
			// 这句话我也不懂是什么意思... 好像没什么用
			conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			// 获取Meta信息对象
			DatabaseMetaData meta = conn.getMetaData();
			// 数据库
			String catalog = null;
			// 数据库的用户
			String schemaPattern = null;// meta.getUserName();
			// 表名
			String tableNamePattern = null;//
			// types指的是table、view
			String[] types = { "TABLE" };
			// Oracle
			if (DATABASETYPE.ORACLE.equals(dbtype)) {
				schemaPattern = username;
				if (null != schemaPattern) {
					schemaPattern = schemaPattern.toUpperCase();
				}
				// 查询
				rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
			} else if (DATABASETYPE.MYSQL.equals(dbtype)) {
				// Mysql查询
				// MySQL 的 table 这一级别查询不到备注信息
				schemaPattern = dbname;
				rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
			}  else if (DATABASETYPE.SQLSERVER.equals(dbtype) || DATABASETYPE.SQLSERVER2005.equals(dbtype)) {
				// SqlServer
				tableNamePattern = "%";
				rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
			}  else if (DATABASETYPE.DB2.equals(dbtype)) {
				// DB2查询
				schemaPattern = "jence_user";
				tableNamePattern = "%";
				rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
			} else if (DATABASETYPE.INFORMIX.equals(dbtype)) {
				// SqlServer
				tableNamePattern = "%";
				rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
			} else if (DATABASETYPE.SYBASE.equals(dbtype)) {
				// SqlServer
				tableNamePattern = "%";
				rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
			}  else if (DATABASETYPE.HIVE.equals(dbtype) || DATABASETYPE.HBASE.equals(dbtype)) {
				// SqlServer
				tableNamePattern = "%";
				rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
			} else {
				throw new RuntimeException("不认识的数据库类型!");
			}
			//
			result = parseResultSetToMapList(rs);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close(rs);
			close(conn, null);
		}
		//
		return result;
	}

	/**
	 * 列出表的所有字段
	 */
	public static List<Map<String, Object>> listColumns(String databasetype, String driverClass, String ip, long port, String dbname,
			String username, String password, String tableName) {
		// 去除首尾空格
		databasetype = trim(databasetype);
		ip = trim(ip);
		dbname = trim(dbname);
		username = trim(username);
		password = trim(password);
		tableName = trim(tableName);
		//
		DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
		//
		List<Map<String, Object>> result = null;
		String url = concatDBURL(dbtype, ip, port, dbname);
		Connection conn = getConnection(driverClass, url, username, password);
		// Statement stmt = null;
		ResultSet rs = null;
		//
		try {
			// 获取Meta信息对象
			DatabaseMetaData meta = conn.getMetaData();
			// 数据库
			String catalog = null;
			// 数据库的用户
			String schemaPattern = null;// meta.getUserName();
			// 表名
			String tableNamePattern = tableName;//
			// 转换为大写
			if (null != tableNamePattern) {
				tableNamePattern = tableNamePattern.toUpperCase();
			}
			// 
			String columnNamePattern = null;
			// Oracle
			if (DATABASETYPE.ORACLE.equals(dbtype)) {
				// 查询
				schemaPattern = username;
				if (null != schemaPattern) {
					schemaPattern = schemaPattern.toUpperCase();
				}
			} else {
				//
			}

			rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
			// TODO 获取主键列,但还没使用
			meta.getPrimaryKeys(catalog, schemaPattern, tableNamePattern);
			//
			result = parseResultSetToMapList(rs);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 关闭资源
			close(rs);
			close(conn, null);
		}
		//
		return result;
	}

	/**
	 * 根据IP,端口,以及数据库名字,拼接Oracle连接字符串
	 * 
	 * @param ip
	 * @param port
	 * @param dbname
	 * @return
	 */
	public static String concatDBURL(DATABASETYPE dbtype, String ip, long port, String dbname) {
		//
		String url = "";
		// Oracle数据库
		if (DATABASETYPE.ORACLE.equals(dbtype)) {
			//
			url += "jdbc:oracle:thin:@";
			url += ip.trim();
			url += ":" + port;
			url += ":" + dbname;
			
			// 如果需要采用 hotbackup
			String url2 = "";
			url2 = url2+"jdbc:oracle:thin:@(DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = "
					+ ip.trim() +")(PORT ="+ port +")))(CONNECT_DATA = (SERVICE_NAME ="+dbname+
					")(FAILOVER_MODE = (TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 5))))";
			//
			// url = url2;
		} else if (DATABASETYPE.MYSQL.equals(dbtype)) {
			//
			url += "jdbc:mysql://";
			url += ip.trim();
			url += ":" + port;
			url += "/" + dbname;
		} else if (DATABASETYPE.SQLSERVER.equals(dbtype)) {
			//
			url += "jdbc:jtds:sqlserver://";
			url += ip.trim();
			url += ":" + port;
			url += "/" + dbname;
			url += ";tds=8.0;lastupdatecount=true";
		} else if (DATABASETYPE.SQLSERVER2005.equals(dbtype)) {
			//
			url += "jdbc:sqlserver://";
			url += ip.trim();
			url += ":" + port;
			url += "; DatabaseName=" + dbname;
		} else if (DATABASETYPE.DB2.equals(dbtype)) {
			url += "jdbc:db2://";
			url += ip.trim();
			url += ":" + port;
			url += "/" + dbname;
		} else if (DATABASETYPE.INFORMIX.equals(dbtype)) {
			// Infox mix 可能有BUG
			url += "jdbc:informix-sqli://";
			url += ip.trim();
			url += ":" + port;
			url += "/" + dbname;
			// +":INFORMIXSERVER=myserver;user="+bean.getDatabaseuser()+";password="+bean.getDatabasepassword()
		} else if (DATABASETYPE.SYBASE.equals(dbtype)) {
			url += "jdbc:sybase:Tds:";
			url += ip.trim();
			url += ":" + port;
			url += "/" + dbname;
		} else if (DATABASETYPE.TERADATA.equals(dbtype)) {
			url += "jdbc:teradata://";
			url += ip.trim();
			url += "/TSNANO=0,CLIENT_CHARSET=cp936,TMODE=TERA,CHARSET=ASCII,DATABASE=" + dbname;
		} else if (DATABASETYPE.VERTICA.equals(dbtype)) {
			//jdbc:vertica://docd04.verticacorp.com:5433/dbname
			//jdbc:vertica://<ip>:5433/<db>
			url += "jdbc:vertica://";
			url += ip.trim();
			url += ":5433";
			url += "/" + dbname;
		} else if (DATABASETYPE.HIVE.equals(dbtype) || DATABASETYPE.HBASE.equals(dbtype)) {
			url += "jdbc:hive2://";
			url += ip.trim();
			url += ":" + port;
			url += "/" + (StringUtils.isEmpty(dbname)?"default":dbname);
		} else {
			throw new RuntimeException("不认识的数据库类型!");
		}
		//
		return url;
	}

	/**
	 * 获取JDBC连接
	 * 
	 * @param url
	 * @param username
	 * @param password
	 * @return
	 */
	public static Connection getConnection(String driverClass, String url, String username, String password) {
		Connection conn = null;
		String poolName = "pool-" + url;
		
		if (ConnectionPoolManager.getInstance().getPool(poolName) == null)
		{
			DBbean beanOracle = new DBbean();  
			beanOracle.setDriverName(driverClass);  
			beanOracle.setUrl(url);  
			beanOracle.setUserName(username);  
			beanOracle.setPassword(password);  
			  
			beanOracle.setMinConnections(15);  
			beanOracle.setMaxConnections(100);  
			  
			beanOracle.setPoolName(poolName);  
			
			ConnectionPoolManager.getInstance().init(beanOracle);
		}
		
		conn = ConnectionPoolManager.getInstance().getConnection(poolName);
		
		/*try {
			Class.forName(driverClass);
			//
			Properties info =new Properties();
			//
			info.put("user", username);
			info.put("password", password);
			// !!! Oracle 如果想要获取元数据 REMARKS 信息,需要加此参数
			info.put("remarksReporting","true");
			// !!! MySQL 标志位, 获取TABLE元数据 REMARKS 信息
			info.put("useInformationSchema","true");
			// 不知道SQLServer需不需要设置...
			//
			
			String key = url + "~" + username + "~" + password;
			if (connPool.containsKey(key))
			{
				conn = DriverManager.getConnection(url, info);
			}
			else
			{
//				DriverManager.getConnection(url, info);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(url + ", " + username + ", " + password);
		}*/
		return conn;
	}

	/**
	 * 将一个未处理的ResultSet解析为Map列表.
	 * 
	 * @param rs
	 * @return
	 */
	public static List<Map<String, Object>> parseResultSetToMapList(ResultSet rs) {
		//
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		//
		if (null == rs) {
			return null;
		}
		//
		try {
			while (rs.next()) {
				// 
				Map<String, Object> map = parseResultSetToMap(rs);
				//
				if (null != map) {
					result.add(map);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//
		return result;
	}

	/**
	 * 解析ResultSet的单条记录,不进行 ResultSet 的next移动处理
	 * 
	 * @param rs
	 * @return
	 */
	private static Map<String, Object> parseResultSetToMap(ResultSet rs) {
		//
		if (null == rs) {
			return null;
		}
		//
		Map<String, Object> map = new HashMap<String, Object>();
		//
		try {
			ResultSetMetaData meta = rs.getMetaData();
			//
			int colNum = meta.getColumnCount();
			//
			for (int i = 1; i <= colNum; i++) {
				// 列名
				String name = meta.getColumnLabel(i); // i+1
				Object value = rs.getObject(i);
				// 加入属性
				map.put(name, value);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//
		return map;
	}
	
	private static TableDto parseResulDto(ResultSet rs, TableDto tableDto) 
	{
		if (null == rs) 
		{
			return null;
		}
		
		try 
		{
			while (rs.next())
			{
				// 列名
				String columnName = rs.getString("COLUMN_NAME");
				String columnType = rs.getString("TYPE_NAME"); 
				int datasize = rs.getInt("COLUMN_SIZE"); 
				
				ColumnsDto colsDto = new ColumnsDto();
				colsDto.setColumnName(columnName);
				colsDto.setSqlType(columnType);
				colsDto.setColumnSize(datasize);
				
				tableDto.getColumnList().add(colsDto);
			}
		}
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
		
		return tableDto;
	}
	
	//
	public static boolean TryLink(String databasetype, String driverClass, String ip, long port, String dbname, String username, String password) {
		//
		DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
		String url = concatDBURL(dbtype, ip, port, dbname);
		Connection conn = null;
		//
		try {
			conn = getConnection(driverClass, url, username, password);
			if(null == conn){
				return false;
			}
			DatabaseMetaData meta =  conn.getMetaData();
			//
			if(null == meta){
				return false;
			} else {
				// 只有这里返回true
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			close(conn, null);
		}
		//
		return false;
	}
	//
	public static void close(Connection conn, String url) {
		if(conn!=null) 
		{
			if (!StringUtils.isEmpty(url))
			{
				ConnectionPoolManager.getInstance().close("pool-" + url, conn);
			}
			else
			{
				try {
					conn.close();
					conn = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//
	public static void close(Statement stmt) {
		if(stmt!=null) {
			try {
				stmt.close();
				stmt = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	//
	public static void close(ResultSet rs) {
		if(rs!=null) {
			try {
				rs.close();
				rs = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	//
	public static String trim(String str){
		if(null != str){
			str = str.trim();
		}
		return str;
	}
}