package org.wikimedia.diffdb.analysis;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang3.mutable.MutableInt;

public class Dataset {
	/*
	 * The key of the hashmap is the year and the second key is the month. 
	 * 
	 * 
	 */
	static String name;
	static Calendar calendar;
	static HashMap<Integer, HashMap<Integer, MutableInt>> container = new HashMap<Integer, HashMap<Integer, MutableInt>>();

	public Dataset(String name) {
		Dataset.name= name; 
		calendar = getCalendar();
	}

	private static Date convertTimestamptoDate(String timestamp) {
		Date date = null;
		DateFormat dt = DateFormat.getDateInstance();
		try {
			date = dt.parse(timestamp);
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
			result = calendar.get(Calendar.MONTH);
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

	public static void addDate(String timestamp) {
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
	
	private static void incrementObs(int year, int month) {
		MutableInt count = container.get(year).get(month);
		count.increment();
		container.get(year).put(month, count);
	}
	
	private static String createFilename() {
		Date date = new Date();
		int year = getComponentFromDate(date, "year");
		int month = getComponentFromDate(date, "month");
		int day = getComponentFromDate(date, "day");
		
		String filename = name + "_" + year + "_" + month + "_" + day;
		
		return filename;
		
	}
	
	public static void writeDataset() throws IOException {
		String filename = createFilename();
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