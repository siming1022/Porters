package com.teamsun.porters.exe;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class CheckTable {

	public static void main(String[] args)
	{
		try
		{
			File file = new File("C:\\Users\\Administrator\\Desktop\\代收货款数据迁移\\10.3.37.18-sqoop.txt");
			
			InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
			BufferedReader br = new BufferedReader(isr);
			String line = "";
			
			while ((line = br.readLine()) != null) 
			{
				int beginIndex = line.indexOf("teamsun/") + 8;
				int endIndex = line.indexOf("/", beginIndex);
				String tableName = line.substring(beginIndex, endIndex);
				System.out.println(tableName);
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}
