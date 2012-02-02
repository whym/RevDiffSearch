package org.wikimedia.revdiffsearch.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class FileUtils{
	static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
	
	public static String createFilename(String desc) {
		String sFileName = null;
		
		Date date = new Date();
		sFileName = dateFormat.format(date) + '_' + desc;
		sFileName = sFileName.replace(" ", "_");
		sFileName = sFileName.replace(":", "_");
		return sFileName;
	}
	
}