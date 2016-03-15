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
import com.teamsun.porters.move.factory.MoveOprationFactory;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.server.MoveServer;
import com.teamsun.porters.move.thread.DataMoveThread;
import com.teamsun.porters.move.util.Constants;
import com.teamsun.porters.move.util.ExcelUtil;
import com.teamsun.porters.move.util.StringUtils;


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
					List<MoveOpration> oprations = getEntitys(ExcelUtil.readExcel(is, true));
					
					int threadCount = oprations.size()%EXEC_COUNT==0?oprations.size()/EXEC_COUNT:(oprations.size()/50)+1;
					ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
					
					for (int i = 0; i < threadCount; i++)
					{
						threadPool.execute(new DataMoveThread(oprations.subList(i * EXEC_COUNT, ((i+1) * EXEC_COUNT) - 1)));
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

	private static List<MoveOpration> getEntitys(List<Map<String, Object>> readExcel) 
	{
		List<MoveOpration> moveOps = new ArrayList<MoveOpration>();
		Map<String, Object> config = null;
		
		for (int i = 1; i < readExcel.size(); i++)
		{
			config = readExcel.get(i);
			
			try 
			{
				ConfigDomain cd = getConfigDto(config);
				
				MoveOpration mo = MoveOprationFactory.create(cd.getType());
				mo.setType(cd.getType());
				mo.setConfigDto(cd);
				
				//对Excel里的参数进行验证，如果验证出错，会抛出异常
				mo.valid();
				
				moveOps.add(mo);
			} 
			catch (BaseException e) 
			{
				e.printStackTrace();
				log.error("valid excel data error, index: " + (i + 1) + ", data: " + config.toString() + " error: " + e.getMessage());
			}
		}
		
		return moveOps;
	}

	private static ConfigDomain getConfigDto(Map<String, Object> config) throws BaseException
	{
		if (StringUtils.isObjEmpty(config.get("dataSouce")))
		{
			throw new BaseException("请选择数据源");
		}

		if (StringUtils.isObjEmpty(config.get("dataDest")))
		{
			throw new BaseException("请选择数据目的");
		}
		
		ConfigDomain configDomain = new ConfigDomain();
		String dataSouce = config.get("dataSouce").toString();
		String dataDest = config.get("dataDest").toString();
		
		transMap2Bean(config, configDomain);
		
		configDomain.setType(dataSouce.toUpperCase() + "2" + dataDest.toUpperCase());
		
		return configDomain;
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