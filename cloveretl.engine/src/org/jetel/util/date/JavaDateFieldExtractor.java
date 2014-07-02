package org.jetel.util.date;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;


public class JavaDateFieldExtractor implements DateFieldExtractor {


	
	/**
	 * Creates new instance of extractor from Java or Joda timezones
	 * 
	 * @param timeZoneString identifier of Java or Joda timezone. Java timezone my optionally contain "java:" prefix. Joda timezone should contain the "joda:" prefix
	 * Timezone string without any prefix is treated as Java prefix
	 */
	
	private Calendar calendar;
	
	public JavaDateFieldExtractor(TimeZone tz) {
		this.calendar = Calendar.getInstance(tz);
	}

	
	public void setDate(Date value) {
		this.calendar.setTime(value);
	}
	
	public int getYear() {
		return calendar.get(Calendar.YEAR);
	}
	
	public int getMonth() {
		return calendar.get(Calendar.MONTH) + 1;
	}
	
	public int getDay() {
		return calendar.get(Calendar.DAY_OF_MONTH);
	}
	
	public int getHour() {
		return calendar.get(Calendar.HOUR_OF_DAY);
	}
	
	public int getMinute() {
		return calendar.get(Calendar.MINUTE);
	}
	
	public int getSecond() {
		return calendar.get(Calendar.SECOND);
	}
	
	public int getMilliSecond() {
		return calendar.get(Calendar.MILLISECOND);
	}
	
}
