package org.jetel.util.date;

import java.util.Date;

public interface DateFieldExtractor {

	
	public static final String JAVA_TZ_PREFIX = "java:";
	public static final String JODA_TZ_PREFIX = "joda:";
	
	public void setDate(Date date);
	public int getYear();	
	public int getMonth();	
	public int getDay();
	public int getHour();
	public int getMinute();
	public int getSecond();	
	public int getMilliSecond();
}
