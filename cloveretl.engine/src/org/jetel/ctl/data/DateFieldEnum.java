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
package org.jetel.ctl.data;

import java.util.Calendar;

import org.jetel.ctl.TransformLangParserConstants;

public enum DateFieldEnum {

	
	DAY(Calendar.DAY_OF_MONTH),
	MONTH(Calendar.MONTH),
	WEEK(Calendar.WEEK_OF_YEAR),
	YEAR(Calendar.YEAR),
	HOUR(Calendar.HOUR),
	MINUTE(Calendar.MINUTE),
	SECOND(Calendar.SECOND),
	MILLISEC(Calendar.MILLISECOND);
	
	private int calendarField;
	
	private DateFieldEnum(int calendarField) {
		this.calendarField = calendarField;
	}
	
	public static final DateFieldEnum fromToken(int token) {
		switch (token) {
		case TransformLangParserConstants.DAY:
			return DateFieldEnum.DAY;
		case TransformLangParserConstants.MONTH:
			return DateFieldEnum.MONTH;
		case TransformLangParserConstants.WEEK:
			return DateFieldEnum.WEEK;
		case TransformLangParserConstants.YEAR:
			return DateFieldEnum.YEAR;
		case TransformLangParserConstants.HOUR:
			return DateFieldEnum.HOUR;
		case TransformLangParserConstants.MINUTE:
			return DateFieldEnum.MINUTE;
		case TransformLangParserConstants.SECOND:
			return DateFieldEnum.SECOND;
		case TransformLangParserConstants.MILLISEC:
			return DateFieldEnum.MILLISEC;
		default:
			throw new IllegalArgumentException("Not a date field symbol: " + token);
		}
	}
	
	public int asCalendarField() {
		return calendarField;
	}
}
