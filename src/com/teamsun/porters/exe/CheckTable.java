package com.teamsun.porters.exe;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class CheckTable {

	public static void main(String[] args)
	{
		try
		{
			File file = new File("C:\\Users\\Administrator\\Desktop\\代收货款数据迁移\\10.1.200.81-sqoop.txt");
//			File file = new File("C:\\Users\\Administrator\\Desktop\\代收货款数据迁移\\10.3.20.212-sqoop.txt");
//			File file = new File("C:\\Users\\Administrator\\Desktop\\代收货款数据迁移\\10.3.37.18-sqoop.txt");
			
			InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
			BufferedReader br = new BufferedReader(isr);
			String line = "";
			
			Set<String> set = new HashSet<String>();
			
			while ((line = br.readLine()) != null) 
			{
//				System.out.println(line);
				int beginIndex = line.indexOf("teamsun/") + 8;
				int endIndex = line.indexOf("/", beginIndex);
				
				if (endIndex == -1)
					endIndex = line.indexOf(" ", beginIndex);
				
				String tableName = line.substring(beginIndex, endIndex);
//				System.out.println(tableName);
				set.add(tableName);
			}
			
			System.out.println(set.size());
			
			/*Iterator<String> it = set.iterator();
			while (it.hasNext())
			{
				System.out.println(it.next());
			}*/
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}
