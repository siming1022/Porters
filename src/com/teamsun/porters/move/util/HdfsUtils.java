package com.teamsun.porters.move.util;

import java.text.MessageFormat;

import com.teamsun.porters.move.template.HdfsTemplate;

/**
 * HDFS命令帮助类
 * 
 * @author Administrator
 *
 */
public class HdfsUtils 
{
	public static String genCopyCommand(String srcHdfsLoc, String destHdfsLoc)
	{
		return MessageFormat.format(HdfsTemplate.HDFS_COPY, srcHdfsLoc, destHdfsLoc);
	}
}
