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
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.porters.move.domain.conf.ConfigDomain;
import com.teamsun.porters.move.exception.BaseException;
import com.teamsun.porters.move.factory.MoveOprationFactory;
import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.thread.DataMoveThread;
import com.teamsun.porters.move.util.ExcelUtil;
import com.teamsun.porters.move.util.StringUtils;
import com.teamsun.porters.move.util.Utils;


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
	public static Properties configPro = Utils.getPropertiesFromFile("config.properties");
	
//	private static final String EXCEL_LOCATION = "~/app/file/";
	private static final String EXCEL_LOCATION = "D://porters";
	private static final int EXEC_COUNT = 200;
	
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
					List<MoveOpration> oprations = getEntitys(ExcelUtil.readExcel(is, false));
					List<DataMoveThread> threads = new ArrayList<DataMoveThread>();
					if (oprations != null && oprations.size() > 0)
					{
						int threadCount = oprations.size()%EXEC_COUNT==0?oprations.size()/EXEC_COUNT:(oprations.size()/EXEC_COUNT)+1;
						ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);

						for (int i = 0; i < threadCount; i++)
						{
							int beginIndex = i * EXEC_COUNT;
							int endIndex = ((i+1) * EXEC_COUNT);
							endIndex = endIndex>=oprations.size()?oprations.size():endIndex;
							DataMoveThread dmt = new DataMoveThread(oprations.subList(beginIndex, endIndex));
							threads.add(dmt);
							threadPool.execute(dmt);
						}
						
						threadPool.shutdown();
					}
					else
					{
						log.error("没有需要执行的操作");
					}
					
					while (true)
					{
						boolean isFinished = false;
						for (DataMoveThread t : threads)
						{
							isFinished = t.isFinished();
							if (!isFinished)
								break;
						}
						
						if (isFinished)
						{
							log.info("system exit");
							System.exit(0);
						}
					}
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
				
				if (cd != null)
				{
					MoveOpration mo = MoveOprationFactory.create(cd.getType());
					
					if (mo == null)
					{
						throw new BaseException("this type " + cd.getType() + " can't create moveOpration");
					}
					
					mo.setType(cd.getType());
					mo.setConfigDto(cd);
					
					//对Excel里的参数进行验证，如果验证出错，会抛出异常
					mo.valid();
					
					moveOps.add(mo);
				}
			} 
			catch (BaseException e) 
			{
				e.printStackTrace();
				log.error("valid excel data error, index: " + i + ", data: " + config.toString() + " error: " + e.getMessage());
			}
		}
		
		return moveOps;
	}

	private static ConfigDomain getConfigDto(Map<String, Object> config) throws BaseException
	{
		//加此判断是因为Excel中第一列是类型枚举
		if (StringUtils.isObjEmpty(config.get("dataSource")) && StringUtils.isObjEmpty(config.get("dataDest")))
		{
			return null;
		}

		if (StringUtils.isObjEmpty(config.get("dataSource")))
		{
			throw new BaseException("请选择数据源");
		}

		if (StringUtils.isObjEmpty(config.get("dataDest")))
		{
			throw new BaseException("请选择数据目的");
		}
		
		ConfigDomain configDomain = new ConfigDomain();
		String dataSource = config.get("dataSource").toString();
		String dataDest = config.get("dataDest").toString();
		
		transMap2Bean(config, configDomain);
		
		configDomain.setType(dataSource.toUpperCase() + "2" + dataDest.toUpperCase());
		
		return configDomain;
	}

	public static void transMap2Bean(Map<String, Object> map, Object obj) throws BaseException
	{
		String key = null;
		Object value = null;
		
		try 
		{
			BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

			for (PropertyDescriptor property : propertyDescriptors) 
			{
				key = property.getName();
//				System.out.println(key);
				if (map.containsKey(key)) 
				{
					value = map.get(key);
//					System.out.println(value.getClass());
					// 得到property对应的setter方法
					if (!StringUtils.isObjEmpty(value))
					{
						Method setter = property.getWriteMethod();
						setter.invoke(obj, value);
					}
				}
			}

		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			System.out.println(key + ": " + value + ": " + value.getClass());
			throw new BaseException(e.getMessage());
		}

		return;

	}
}
