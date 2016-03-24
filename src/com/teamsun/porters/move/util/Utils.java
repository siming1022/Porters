package com.teamsun.porters.move.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 帮助类
 * 
 * @author Administrator
 *
 */
public class Utils 
{
	public static Logger log = LoggerFactory.getLogger(Utils.class);

	public static Properties getPropertiesFromFile(String fileName) 
	{
		InputStream is = Utils.class.getClassLoader().getResourceAsStream(fileName);
		Properties p = new Properties();
		try 
		{
			p.load(is);
			is.close();
		} 
		catch (IOException e) 
		{
			log.error("ERROR", e);
		}
		return p;
	}

	public static void saveProperties(Properties p, String fileName) 
	{
		FileOutputStream os;
		try 
		{
			String filePath = Utils.class.getClassLoader().getResource(fileName).getPath();
			os = new FileOutputStream(filePath);
			p.store(os, "stored at: " + new Date());
			os.close();
		} 
		catch (FileNotFoundException e) 
		{
			log.error("ERROR", e);
		} 
		catch (IOException e) 
		{
			log.error("ERROR", e);
		}
	}

	public static Properties refreshProperties(Properties p, String fileName) 
	{
		saveProperties(p, fileName);
		return getPropertiesFromFile(fileName);
	}
}
