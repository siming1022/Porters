package com.teamsun.porters.move.util;

import java.text.MessageFormat;

import com.teamsun.porters.move.domain.HdfsDto;
import com.teamsun.porters.move.domain.BaseMoveDomain;
import com.teamsun.porters.move.template.HdfsTemplate;

/**
 * HDFS命令帮助类
 * 
 * @author Administrator
 *
 */
public class HdfsUtils 
{
	public String genCopyCommand(BaseMoveDomain srcDo, BaseMoveDomain destDo)
	{
		HdfsDto srcDto = (HdfsDto)srcDo;
		HdfsDto destDto = (HdfsDto)destDo;
		
		return MessageFormat.format(HdfsTemplate.HDFS_COPY, srcDto.getHdfsLoc(), destDto.getHdfsLoc());
	}
}
