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
