package org.jetel.util.date;

import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class JodaDateFieldExtractor implements DateFieldExtractor {

	private DateTime date;
	private DateTimeZone tz;

	public JodaDateFieldExtractor(DateTimeZone tz) {
		this.tz = tz;
	}
	
	
	@Override
	public void setDate(Date date) {
		this.date = date == null ? null : new DateTime(date,tz);
	}

	@Override
	public int getYear() {
		return date.getYear();
	}

	@Override
	public int getMonth() {
		return date.getMonthOfYear();
	}

	@Override
	public int getDay() {
		return date.getDayOfMonth();
	}

	@Override
	public int getHour() {
		return date.getHourOfDay();
	}

	@Override
	public int getMinute() {
		return date.getMinuteOfHour();
	}

	@Override
	public int getSecond() {
		return date.getSecondOfMinute();
	}

	@Override
	public int getMilliSecond() {
		return date.getMillisOfSecond();
	}

}
