package org.wikimedia.diffdb.analysis;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang3.mutable.MutableInt;
import org.wikimedia.diffdb.utils.FileUtils;

public class Dataset {
	/*
	 * The key of the hashmap is the year and the second key is the month. 
	 * 
	 * 
	 */
	static String name;
	static String pattern = "yyyy-MM-dd'T'HH:mm:ss";
	static Calendar calendar;
	static HashMap<Integer, HashMap<Integer, MutableInt>> container = new HashMap<Integer, HashMap<Integer, MutableInt>>();

	public Dataset(String dataset_name) {
		name= dataset_name; 
		calendar = getCalendar();
	}

	public boolean containsYear(int year) {
		if (container.containsKey(year)) {
			return true;
		}
		return false;
	}
	
	public boolean containsMonth(int year, int month) {
		HashMap<Integer, MutableInt> result = container.get(year);
		if (result.containsKey(month)) {
			return true;
		}
		return false;
	}
	
	public static Date convertTimestamptoDate(String timestamp) {
		Date date = null;
		
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		try {
			date = sdf.parse(timestamp);
			System.out.println(date.toString());
		} catch (ParseException e) {
			System.out.println("Could not parse " + timestamp.toString());
		}
		return date;
	}

	private static Calendar getCalendar() {
		TimeZone tz = TimeZone.getTimeZone("UTC");
		Calendar calendar = Calendar.getInstance(tz);
		return calendar;
	}
	
	
	private static int getComponentFromDate(Date date, String key) {
		int result = 0;
		calendar.setTime(date);
		if (key == "year") {
			result = calendar.get(Calendar.YEAR);
		} else if (key=="month"){ 
			//ARRGGHHH.. January is 0, so we need to add +1
			result = calendar.get(Calendar.MONTH)+1;
		} else if (key=="day"){
			result = calendar.get(Calendar.DAY_OF_MONTH);
		} else {
			System.out.println(key.toString() + " is an invalid key, please choose from year, month or day");
		}
		return result;
	}
	
//	private static int getMonthFromDate(Date date) {
//		int result = -1;
//		if (date != null) {
//			//Calendar calendar = getCalendar();
//			calendar.setTime(date);
//			result = calendar.get(Calendar.MONTH);
//		}
//		return result;
//	}	
//	
//	private static int getYearFromDate(Date date) {
//		int result = -1;
//		if (date != null) {
//			//Calendar calendar = getCalendar();
//			calendar.setTime(date);
//			result = calendar.get(Calendar.YEAR);
//		}
//		return result;
//	}

	public void addDate(String timestamp) {
		Date date = convertTimestamptoDate(timestamp);
		int year = getComponentFromDate(date, "year");
		int month = getComponentFromDate(date, "month");
		if (!container.containsKey(year)) {
			container.put(year, new HashMap<Integer, MutableInt>());
		}
		
		if (!container.get(year).containsKey(month)) {
			MutableInt count = new MutableInt();
			container.get(year).put(month, count);
		}
		
		incrementObs(year, month);
	}
	
	private void incrementObs(int year, int month) {
		MutableInt count = container.get(year).get(month);
		count.increment();
		container.get(year).put(month, count);
	}
	
	public static void writeDataset() throws IOException {
		String filename = FileUtils.createFilename(name);
		FileWriter fstream = new FileWriter(filename);
		
		Set<Integer> years = container.keySet();
		for (int year: years) {
			Set<Integer> months = container.get(year).keySet();
			for (int month: months) {
				MutableInt count = container.get(year).get(month);
				String row = String.format("%s\t%s\t%s\n", year, month, count.toString());
				fstream.write(row);
			}
		}
		
		fstream.close();
	}
}