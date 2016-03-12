package com.teamsun.porters.exe;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.server.MoveServer;
import com.teamsun.porters.move.thread.DataMoveThread;
import com.teamsun.porters.move.util.Constants;
import com.teamsun.porters.move.util.ExcelUtil;
import com.teamsun.porters.move.util.StringUtils;
import com.teamsun.porters.move.valid.ValidConfig;


/**
 * 主方法
 * 
 * （我们不生产数据，我们只是数据的搬运工）
 * @author Administrator
 *
 */
public class MoveMain 
{
	private static Logger log = LoggerFactory.getLogger(MoveMain.class);
	private static final String EXCEL_LOCATION = "~/app/file/";
	private static final int EXEC_COUNT = 50;
	
	@Resource
	private MoveServer moveServer;
	
	public static void main(String[] args) 
	{
		log.info("read excel begin");
		File baseDir = new File(EXCEL_LOCATION);
		
		if (baseDir != null && baseDir.exists() && baseDir.isDirectory())
		{
			File[] excelFiles = baseDir.listFiles(new FileFilter() {
				
				@Override
				public boolean accept(File pathname) {
					if (pathname.getName().toLowerCase().endsWith(".xlsx") || pathname.getName().toLowerCase().endsWith(".xls"))
						return true;
					
					return false;
				}
			});
			
			if (excelFiles.length == 0)
			{
				log.error("请将配置文件放入【" + EXCEL_LOCATION + "】目录中");
				return;
			}
			
			
			for (File excelFile : excelFiles)
			{
				log.info("read file " + excelFile.getName());
				InputStream is = null;
				
				try 
				{
					is = new FileInputStream(excelFile);
					List<ConfigDomain> configDtos = getEntitys(ExcelUtil.readExcel(is, true));
					
					int threadCount = configDtos.size()%EXEC_COUNT==0?configDtos.size()/EXEC_COUNT:(configDtos.size()/50)+1;
					ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
					
					for (int i = 0; i < threadCount; i++)
					{
						threadPool.execute(new DataMoveThread(configDtos.subList(i * EXEC_COUNT, ((i+1) * EXEC_COUNT) - 1)));
					}
					
					threadPool.shutdown();
				}
				catch (Exception e) 
				{
					e.printStackTrace();
				}
				finally
				{
					try 
					{
						if (is != null)
							is.close();
					}
					catch (IOException e) 
					{
						e.printStackTrace();
					}
				}
			}
		}
		else
		{
			log.error("请检查该目录【" + EXCEL_LOCATION + "】是否存在");
		}
	}

	private static List<ConfigDomain> getEntitys(List<Map<String, Object>> readExcel) 
	{
		List<ConfigDomain> configDatas = new ArrayList<ConfigDomain>();
		Map<String, Object> config = null;
		
		for (int i = 1; i < readExcel.size(); i++)
		{
			config = readExcel.get(i);
			
			try 
			{
				validConfig(config);
				
				ConfigDomain cd = getEntity(config);
				configDatas.add(cd);
			} 
			catch (BaseException e) 
			{
				e.printStackTrace();
				log.error("valid excel data error, index: " + (i + 1) + ", data: " + config.toString() + " error: " + e.getMessage());
			}
		}
		
		return configDatas;
	}

	private static ConfigDomain getEntity(Map<String, Object> config) 
	{
		ConfigDomain configDomain = new ConfigDomain();
		String dataSouce = config.get("dataSouce").toString();
		String dataDest = config.get("dataDest").toString();
		
		transMap2Bean(config, configDomain);
		
		configDomain.setType(dataSouce.toUpperCase() + "2" + dataDest.toUpperCase());
		
		return configDomain;
	}

	private static void validConfig(Map<String, Object> config) throws BaseException
	{
		String dataSource = null;
		String dataDest = null;
		
		if (StringUtils.isObjEmpty(dataSource))
		{
			throw new BaseException("请选择数据源");
		}

		if (StringUtils.isObjEmpty(dataDest))
		{
			throw new BaseException("请选择数据目的");
		}
		
		String type = dataSource.toUpperCase() + "2" + dataDest.toUpperCase();
		
		if (Constants.DATA_SOUCE_TYPE_H2H.equals(type))
		{
			ValidConfig.validH2H(config); 
		}
		else if (Constants.DATA_SOUCE_TYPE_H2O.equals(type))
		{
			ValidConfig.validH2O(config); 
		}
		else if (Constants.DATA_SOUCE_TYPE_H2T.equals(type))
		{
			ValidConfig.validH2T(config); 
		}
		else if (Constants.DATA_SOUCE_TYPE_H2HB.equals(type))
		{
			ValidConfig.validH2HB(config); 
		}
		else if (Constants.DATA_SOUCE_TYPE_H2V.equals(type))
		{
			ValidConfig.validH2V(config); 
		}
		else if (Constants.DATA_SOUCE_TYPE_H2M.equals(type))
		{
			ValidConfig.validH2M(config); 
		}
	}
	
	public static void transMap2Bean(Map<String, Object> map, Object obj) 
	{

		try 
		{
			BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

			for (PropertyDescriptor property : propertyDescriptors) 
			{
				String key = property.getName();

				if (map.containsKey(key)) 
				{
					Object value = map.get(key);
					// 得到property对应的setter方法
					Method setter = property.getWriteMethod();
					setter.invoke(obj, value);
				}
			}

		} 
		catch (Exception e) 
		{
			log.error("transMap2Bean Error " + e);
		}

		return;

	}
}
