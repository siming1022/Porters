package com.teamsun.porters.exe;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

public class GenPro {

	public static void main(String[] args) 
	{
		try
		{
			File file = new File("C:\\Users\\Administrator\\Desktop\\代收货款数据迁移\\porters.txt");
			
			InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
			BufferedReader br = new BufferedReader(isr);
			String line = "";
			
			while ((line = br.readLine()) != null) 
			{
				String[] props = line.split("\t");
//				Oracle	HDFS	TB_EVT_ROUTE_SEQ			MWAY_CODE,SITE_SEQ		/EMS_Data/teamsun/test/TB_EVT_ROUTE_SEQ_TEST3			10.3.51.192		1521		RDBDB1		emscust		Sdjdmt9t#			

				String prop = "Oracle" + "\t"
						+ "HDFS" + "\t"
						+ props[4]
						+ "\t" 
						+ "\t" 
						+ "\t" 
						+ "\t"
						+ "\t"
						+ "/EMS_Data/teamsun/" + props[4].toUpperCase() + "\t"
						+ "\t" 
						+ "\t"
						+ props[0]
						+ "\t"
						+ "\t"
						+ "1521"
						+ "\t"
						+ "\t"
						+ props[1]
						+ "\t"
						+ "\t"
						+ props[2]
						+ "\t"
						+ "\t"
						+ props[3]
						+ "\t" 
						+ "\t" 
						+ "\t" 
						+ "\t" 
						+ "30";
				
				writeToFile(prop);
//				System.out.println();
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	private static void writeToFile(String string) 
	{
		try 
		{
			File f = new File("C:\\Users\\Administrator\\Desktop\\代收货款数据迁移\\prop.txt");
			FileWriter wr = new FileWriter(f, true);
			wr.write(new String(string.getBytes("UTF-8")) + "\r\n");
			wr.close();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}
