package com.teamsun.porters.move.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.POIXMLDocument;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * @Description:Excel读取工具
 * @author evall
 * @ClassName: ExcelUtil 
 * @version 2011-3-23 下午03:22:47
 */
public class ExcelUtil {
	
	public static List<Map<String, Object>> readExcel(InputStream in, boolean isRtnColumnMap) throws InvalidFormatException, IOException, ParseException
	{
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		
		if (!in.markSupported()) 
		{
			   in = new PushbackInputStream(in, 8);
		}
		
		//Excel 2003
		if (POIFSFileSystem.hasPOIFSHeader(in))
		{
			list = read03Excel(in, isRtnColumnMap);
		}
		//Excel 2007
		else if (POIXMLDocument.hasOOXMLHeader(in))
		{
			
			list = read07Excel(in, isRtnColumnMap);
		}
		
		
		return list;
	}
	
	/**
	 * 读取Excel 2003版本
	 * 
	 * @param in
	 * @param isRtnColumnMap
	 * @return
	 * @throws IOException
	 */
	private static List<Map<String, Object>> read03Excel(InputStream in, boolean isRtnColumnMap) throws IOException, ParseException
	{
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		
		HSSFWorkbook workbook = new HSSFWorkbook(in);
		HSSFSheet sheet = workbook.getSheetAt(0);
		int rows = sheet.getPhysicalNumberOfRows();

		if(rows>0){
			HSSFRow row = sheet.getRow(0);
			int cells = row.getPhysicalNumberOfCells();
			ArrayList<String> head = new ArrayList<String>();
			for (int i = 0; i < cells; i++) {
				head.add(row.getCell((short) i).getStringCellValue());
			}
	
			for (int i = 1; i < rows; i++) {
				HashMap<String, Object> map = new HashMap<String, Object>();
				map.put("row", i+1);
				row = sheet.getRow(i);
				if(row==null) continue;
				for (int j = 0; j < cells; j++) {
					Object v = null;
					HSSFCell cell = row.getCell((short) j);
					if (cell != null) {
						switch (cell.getCellType()) {
						case HSSFCell.CELL_TYPE_NUMERIC: {
							if (HSSFDateUtil.isCellDateFormatted(cell)) {
								v = dateFormat.format(cell.getDateCellValue());
							} else {
								double d = cell.getNumericCellValue();
								v = formator.parse(formator.format(d));
							}
							break;
						}
						case HSSFCell.CELL_TYPE_STRING:
							v = cell.getStringCellValue();
							v = v==null?v:v.toString().trim();
							break;
						default:
							v = "";
						}
					} else {
						v = "";
					}
					if(!isRtnColumnMap)
						map.put(head.get(j), v);
					else
						map.put(String.valueOf(j+1), v);
				}
				list.add(map);
			}
		}
		
		return list;
	}

	/**
	 * 读取Excel 2007版本
	 * 
	 * @param in
	 * @param isRtnColumnMap
	 * @return
	 * @throws IOException
	 * @throws InvalidFormatException 
	 */
	private static List<Map<String, Object>> read07Excel(InputStream in, boolean isRtnColumnMap) throws IOException, ParseException, InvalidFormatException
	{
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		
		XSSFWorkbook workbook = new XSSFWorkbook(OPCPackage.open(in));
		XSSFSheet sheet = workbook.getSheetAt(0);
		int rows = sheet.getPhysicalNumberOfRows();
		
		if(rows>0){
			XSSFRow row = sheet.getRow(0);
			int cells = row.getPhysicalNumberOfCells();
			ArrayList<String> head = new ArrayList<String>();
			for (int i = 0; i < cells; i++) {
				head.add(row.getCell((short) i).getStringCellValue());
			}
			
			for (int i = 1; i < rows; i++) {
				HashMap<String, Object> map = new HashMap<String, Object>();
				map.put("row", i+1);
				row = sheet.getRow(i);
				if(row==null) continue;
				for (int j = 0; j < cells; j++) {
					Object v = null;
					XSSFCell cell = row.getCell((short) j);
					if (cell != null) {
						switch (cell.getCellType()) {
						case XSSFCell.CELL_TYPE_NUMERIC: {
							if (HSSFDateUtil.isCellDateFormatted(cell)) {
								v = dateFormat.format(cell.getDateCellValue());
							} else {
								double d = cell.getNumericCellValue();
								v = formator.parse(formator.format(d));
							}
							break;
						}
						case XSSFCell.CELL_TYPE_STRING:
							v = cell.getStringCellValue();
							v = v==null?v:v.toString().trim();
							break;
						default:
							v = "";
						}
					} else {
						v = "";
					}
					if(!isRtnColumnMap)
						map.put(head.get(j), v);
					else
						map.put(String.valueOf(j+1), v);
				}
				list.add(map);
			}
		}
		
		return list;
	}
	
	private final static NumberFormat formator = NumberFormat.getInstance();
	private final static SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
}