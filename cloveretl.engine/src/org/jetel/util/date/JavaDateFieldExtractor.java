/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
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

	
	@Override
	public void setDate(Date value) {
		this.calendar.setTime(value);
	}
	
	@Override
	public int getYear() {
		return calendar.get(Calendar.YEAR);
	}
	
	@Override
	public int getMonth() {
		return calendar.get(Calendar.MONTH) + 1;
	}
	
	@Override
	public int getDay() {
		return calendar.get(Calendar.DAY_OF_MONTH);
	}
	
	@Override
	public int getHour() {
		return calendar.get(Calendar.HOUR_OF_DAY);
	}
	
	@Override
	public int getMinute() {
		return calendar.get(Calendar.MINUTE);
	}
	
	@Override
	public int getSecond() {
		return calendar.get(Calendar.SECOND);
	}
	
	@Override
	public int getMilliSecond() {
		return calendar.get(Calendar.MILLISECOND);
	}
	
}
