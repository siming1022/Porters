package com.teamsun.porters.move.util;

public class StringUtils 
{
	public static boolean isEmpty(String input) 
	{
		return (input == null || input.length() == 0);
	}
	
	public static boolean isEmptyWithTrim(String input) 
	{
		return (input == null || input.trim().length() == 0);
	}
	
	public static boolean isObjEmpty(Object obj)
	{
		return obj==null?true:isEmpty(obj.toString());
	}
	
	public static String objToString(Object obj)
	{
		return obj==null?"":obj.toString();
	}
	
	public static String getValue(String value,String defaultString) 
	{
		return getValue(value, defaultString, !isEmpty(value));
	}
	
	public static String getValue(String value,String defaultString,boolean isTrue) 
	{
		if(isTrue)return value;
		return defaultString;
	}
}
