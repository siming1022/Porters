package com.teamsun.porters.exe;

import java.sql.Connection;
import java.sql.DriverManager;

public class Test {

	public static void main(String[] args) throws Exception
	{
		
		
//		(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 10.3.37.18)(PORT = 1521))(CONNECT_DATA = (INSTANCE_NAME = outtrack1) (SERVER = dedicated) (SERVICE_NAME = outtrack))) 
		Class.forName("oracle.jdbc.driver.OracleDriver");
//		String url = "jdbc:oracle:thin:@10.3.37.18:1521:DSHK_OUTTRACK";
//		String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST = 10.3.37.18)(PORT = 1521))(CONNECT_DATA = (INSTANCE_NAME = outtrack1) (SERVER = dedicated) (SERVICE_NAME = outtrack)))";
//		String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(FAILOVER=on)(ADDRESS=(PROTOCOL=TCP)(HOST=10.3.20.212)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=10.3.20.213)(PORT=1521)))(CONNECT_DATA=(FAILOVER_MODE=(TYPE=select)(METHOD=basic))(SERVER=dedicated)(SERVICE_NAME=swdb)))";
		String url = "jdbc:oracle:thin:@  (DESCRIPTION=    (ADDRESS=      (PROTOCOL=TCP)      (HOST=10.1.200.80)      (PORT=1521)    )    (CONNECT_DATA=      (SID=TDDB1)    )  )";	
		String usrname = "bireport";
		String password = "bireport321";
		
		Connection con = DriverManager.getConnection(url, usrname, password);
	}

}
