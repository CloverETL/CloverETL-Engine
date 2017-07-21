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
package org.jetel.ctl.extensions;

import java.util.Calendar;
import java.util.Date;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.data.DateFieldEnum;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;

public class DateLib extends TLFunctionLibrary {

    @Override
    public TLFunctionPrototype getExecutable(String functionName) {
    	final TLFunctionPrototype ret = 
    		"dateDiff".equals(functionName) ? new DateDiffFunction() : 
    		"dateAdd".equals(functionName) ? new DateAddFunction() :
    		"today".equals(functionName) ? new TodayFunction() :
    		"zeroDate".equals(functionName) ? new ZeroDateFunction() :
    		"extractDate".equals(functionName) ? new ExtractDateFunction() :
    		"extractTime".equals(functionName) ? new ExtractTimeFunction() :
    		"trunc".equals(functionName) ? new TruncFunction() :
    	    "truncDate".equals(functionName)? new TruncDateFunction() : 
        	"getDay".equals(functionName)? new GetDayFunction() : 
       	    "getMonth".equals(functionName)? new GetMonthFunction() : 
            "getYear".equals(functionName)? new GetYearFunction() : 
            "getHour".equals(functionName)? new GetHourFunction() : 
            "getMinute".equals(functionName)? new GetMinuteFunction() : 
            "getSecond".equals(functionName)? new GetSecondFunction() : 
            "getMillisecond".equals(functionName)? new GetMillisecondFunction() : 
            "createDate".equals(functionName)? new CreateDateFunction() : 
    	    null;
    		
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
    
		return ret;
    }
    
	private static String LIBRARY_NAME = "Date";

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}


    @TLFunctionAnnotation("Returns current date and time.")
    public static final Date today(TLFunctionCallContext context) {
    	return new Date();
    }

    // TODAY
    class TodayFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(today(context));
		}
    	
    }
        
    // DATEADD
	class DateAddFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			dateAddInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final DateFieldEnum unit = (DateFieldEnum)stack.pop();
			final Long shift = stack.popLong();
			final Date lhs = stack.popDate();
			
			stack.push(dateAdd(context, lhs,shift,unit));
		}
	}
	
	@TLFunctionInitAnnotation
    public static final void dateAddInit(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache(context));
    }
    
    @TLFunctionAnnotation("Adds to a component of a date (e.g. month)")
    public static final Date dateAdd(TLFunctionCallContext context, Date lhs, Long shift, DateFieldEnum unit) {
    	Calendar c = ((TLCalendarCache) context.getCache()).getCalendar();
    	c.setTime(lhs);
    	c.add(unit.asCalendarField(), shift.intValue());
    	
        return c.getTime();
    }

	
	// DATEDIFF
    class DateDiffFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			dateDiffInit(context);
		}

    	@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
    		final DateFieldEnum unit = (DateFieldEnum)stack.pop();
    		final Date rhs = stack.popDate();
    		final Date lhs = stack.popDate();
    		
    		stack.push(dateDiff(context, lhs, rhs, unit));
    	}

	}
    
    @TLFunctionInitAnnotation
    public static final void dateDiffInit(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache(context));
    }
    
    @TLFunctionAnnotation("Returns the difference between dates")
    public static final Long dateDiff(TLFunctionCallContext context, Date lhs, Date rhs, DateFieldEnum unit) {
    	if (unit == DateFieldEnum.MILLISEC) { // CL-1087
    		return lhs.getTime() - rhs.getTime();
    	}
    	
        long diff = 0;
        switch (unit) {
        case SECOND:
            // we have the difference in seconds
        	diff = (long) Seconds.secondsBetween(new DateTime(rhs.getTime()), new DateTime(lhs.getTime())).getSeconds();
            break;
        case MINUTE:
            // how many minutes'
        	diff = (long) Minutes.minutesBetween(new DateTime(rhs.getTime()), new DateTime(lhs.getTime())).getMinutes();
            break;
        case HOUR:
        	diff = (long) Hours.hoursBetween(new DateTime(rhs.getTime()), new DateTime(lhs.getTime())).getHours();
            break;
        case DAY:
            // how many days is the difference
        	diff = (long) Days.daysBetween(new DateTime(rhs.getTime()), new DateTime(lhs.getTime())).getDays();
            break;
        case WEEK:
            // how many weeks
        	diff = (long) Weeks.weeksBetween(new DateTime(rhs.getTime()), new DateTime(lhs.getTime())).getWeeks();
            break;
        case MONTH:
        	diff = (long) Months.monthsBetween(new DateTime(rhs.getTime()), new DateTime(lhs.getTime())).getMonths();
            break;
        case YEAR:
        	diff = (long) Years.yearsBetween(new DateTime(rhs.getTime()), new DateTime(lhs.getTime())).getYears();
            break;
        default:
            throw new TransformLangExecutorRuntimeException("Unknown time unit " + unit);
        }
        
        return diff;
    }
	
    @TLFunctionAnnotation("Returns 1.1.1970 date.")
    public static final Date zeroDate(TLFunctionCallContext context) {
    	return new Date(0L);
    }
    
    class ZeroDateFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(zeroDate(context));
		}
    }
    

    
    // extractDate
    class ExtractDateFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			extractDateInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(extractDate(context, stack.popDate()));
		}
	}

    @TLFunctionInitAnnotation
    public static final void extractDateInit(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache(context));
    }
    
    @TLFunctionAnnotation("Extracts only date portion from date-time value, setting all time fields to zero.")
	public static final Date extractDate(TLFunctionCallContext context, Date d) {
    	// this hardcore code is necessary, subtracting milliseconds 
    	// or using Calendar.clear() does not seem to handle light-saving correctly
    	if (d == null){
    		return null;
    	}
    	Calendar cal = ((TLCalendarCache)context.getCache()).getCalendar();
    	cal.setTime(d);
    	int[] portion = new int[]{cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.MONTH), cal.get(Calendar.YEAR)};
    	cal.clear();
    	cal.set(Calendar.DAY_OF_MONTH, portion[0]);
    	cal.set(Calendar.MONTH, portion[1]);
    	cal.set(Calendar.YEAR, portion[2]);
    	return cal.getTime();
    }
    
    // extractTime
    class ExtractTimeFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			extractTimeInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(extractTime(context, stack.popDate()));
		}
	}

    @TLFunctionInitAnnotation
	public static void extractTimeInit(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache(context));
	}

    @TLFunctionAnnotation("Extracts only time portion from date-time value, clearing all date fields.")
	public static final Date extractTime(TLFunctionCallContext context, Date d) {
    	// this hardcore code is necessary, subtracting milliseconds 
    	// or using Calendar.clear() does not seem to handle light-saving correctly
    	Calendar cal = ((TLCalendarCache)context.getCache()).getCalendar();
    	if (d == null){
    		return null;
    	}
    	cal.setTime(d);
    	int[] portion = new int[]{cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),cal.get(Calendar.MILLISECOND)};
    	cal.clear();
    	cal.set(Calendar.HOUR_OF_DAY, portion[0]);
    	cal.set(Calendar.MINUTE, portion[1]);
    	cal.set(Calendar.SECOND, portion[2]);
    	cal.set(Calendar.MILLISECOND, portion[3]);
    	return cal.getTime();
    }

    //Trunc
    class TruncFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			truncInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(trunc(context, stack.popDate()));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void truncInit(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache(context));
    }

    @Deprecated
    @TLFunctionAnnotation("Deprecated, use extractDate() instead. The function modifies its argument.\nTruncates other, but date-time values to zero.")
    public static final Date trunc(TLFunctionCallContext context, Date date) {
    	Calendar cal = ((TLCalendarCache)context.getCache()).getCalendar();
    	cal.setTime(date);
    	int[] portion = new int[]{cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.MONTH), cal.get(Calendar.YEAR)};
    	cal.clear();
    	cal.set(Calendar.DAY_OF_MONTH, portion[0]);
    	cal.set(Calendar.MONTH, portion[1]);
    	cal.set(Calendar.YEAR, portion[2]);
    	date.setTime(cal.getTimeInMillis());
    	return date;
    }
    
    //Trunc date
    class TruncDateFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			truncDateInit(context);			
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(truncDate(context, stack.popDate()));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void truncDateInit(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache(context));
    }

    @Deprecated
    @TLFunctionAnnotation("Deprecated, use extractTime() instead. The function modifies its argument.\nTruncates other, but day time values to zero.")
    public static final Date truncDate(TLFunctionCallContext context, Date date) {
    	Calendar cal = ((TLCalendarCache)context.getCache()).getCalendar();
    	cal.setTime(date);
    	int[] portion = new int[]{cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),cal.get(Calendar.MILLISECOND)};
    	cal.clear();
    	cal.set(Calendar.HOUR_OF_DAY, portion[0]);
    	cal.set(Calendar.MINUTE, portion[1]);
    	cal.set(Calendar.SECOND, portion[2]);
    	cal.set(Calendar.MILLISECOND, portion[3]);
    	date.setTime(cal.getTimeInMillis());
    	return date;
    }

    //getDay
    class GetDayFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			getDayInit(context);			
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String timeZone = null;
			if (context.getParams().length > 1) {
				timeZone = stack.popString();
			}
			stack.push(getDay(context, stack.popDate(), timeZone));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void getDayInit(TLFunctionCallContext context) {
    	context.setCache(TLCalendarCache.withTimeZone(context, 1));
    }
    
    @TLFunctionAnnotation("Returns the day of the month using the default time zone. The first day of the month has the value 1.")
    public static final Integer getDay(TLFunctionCallContext context, Date date) {
    	return getDay(context, date, null);
    }
    
    @TLFunctionAnnotation("Returns the day of the month using the specified time zone. The first day of the month has the value 1.")
    public static final Integer getDay(TLFunctionCallContext context, Date date, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 1);
    	if (date == null){
    		return null;
    	}
    	cal.setTime(date);
    	return cal.get(Calendar.DAY_OF_MONTH);
    }
    
    //getMonth
    class GetMonthFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			getMonthInit(context);			
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String timeZone = null;
			if (context.getParams().length > 1) {
				timeZone = stack.popString();
			}
			stack.push(getMonth(context, stack.popDate(), timeZone));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void getMonthInit(TLFunctionCallContext context) {
    	context.setCache(TLCalendarCache.withTimeZone(context, 1));
    }

    @TLFunctionAnnotation("Returns the month using the default time zone. The first month of the year has the value 1.")
    public static final Integer getMonth(TLFunctionCallContext context, Date date) {
    	return getMonth(context, date, null);
    }

    @TLFunctionAnnotation("Returns the month using the specified time zone. The first month of the year has the value 1.")
    public static final Integer getMonth(TLFunctionCallContext context, Date date, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 1);
    	if(date == null){
    		return null;
    	}
    	cal.setTime(date);
    	return cal.get(Calendar.MONTH) + 1;
    }

    //getYear
    class GetYearFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			getYearInit(context);			
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String timeZone = null;
			if (context.getParams().length > 1) {
				timeZone = stack.popString();
			}
			stack.push(getYear(context, stack.popDate(), timeZone));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void getYearInit(TLFunctionCallContext context) {
    	context.setCache(TLCalendarCache.withTimeZone(context, 1));
    }

    @TLFunctionAnnotation("Returns the year using the default time zone.")
    public static final Integer getYear(TLFunctionCallContext context, Date date) {
    	return getYear(context, date, null);
    }

    @TLFunctionAnnotation("Returns the year using the specified time zone.")
    public static final Integer getYear(TLFunctionCallContext context, Date date, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 1);
    	if (date == null){
    		return null;
    	}
    	cal.setTime(date);
    	return cal.get(Calendar.YEAR);
    }

    //getHour
    class GetHourFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			getHourInit(context);			
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String timeZone = null;
			if (context.getParams().length > 1) {
				timeZone = stack.popString();
			}
			stack.push(getHour(context, stack.popDate(), timeZone));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void getHourInit(TLFunctionCallContext context) {
    	context.setCache(TLCalendarCache.withTimeZone(context, 1));
    }

    @TLFunctionAnnotation("Returns the hour of the day (24-hour clock) using the default time zone.")
    public static final Integer getHour(TLFunctionCallContext context, Date date) {
    	return getHour(context, date, null);
    }

    @TLFunctionAnnotation("Returns the hour of the day (24-hour clock) using the specified time zone.")
    public static final Integer getHour(TLFunctionCallContext context, Date date, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 1);
    	if (date == null){
    		return null;
    	}
    	cal.setTime(date);
    	return cal.get(Calendar.HOUR_OF_DAY);
    }

    //getMinute
    class GetMinuteFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			getMinuteInit(context);			
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String timeZone = null;
			if (context.getParams().length > 1) {
				timeZone = stack.popString();
			}
			stack.push(getMinute(context, stack.popDate(), timeZone));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void getMinuteInit(TLFunctionCallContext context) {
    	context.setCache(TLCalendarCache.withTimeZone(context, 1));
    }

    @TLFunctionAnnotation("Returns the minute within the hour using the default time zone.")
    public static final Integer getMinute(TLFunctionCallContext context, Date date) {
    	return getMinute(context, date, null);
    }

    @TLFunctionAnnotation("Returns the minute within the hour using the specified time zone.")
    public static final Integer getMinute(TLFunctionCallContext context, Date date, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 1);
    	if (date == null){
    		return null;
    	}
    	cal.setTime(date);
    	return cal.get(Calendar.MINUTE);
    }

    //getSecond
    class GetSecondFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			getSecondInit(context);			
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String timeZone = null;
			if (context.getParams().length > 1) {
				timeZone = stack.popString();
			}
			stack.push(getSecond(context, stack.popDate(), timeZone));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void getSecondInit(TLFunctionCallContext context) {
    	context.setCache(TLCalendarCache.withTimeZone(context, 1));
    }

    @TLFunctionAnnotation("Returns the second within the minute using the default time zone.")
    public static final Integer getSecond(TLFunctionCallContext context, Date date) {
    	return getSecond(context, date, null);
    }

    @TLFunctionAnnotation("Returns the second within the minute using the specified time zone.")
    public static final Integer getSecond(TLFunctionCallContext context, Date date, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 1);
    	if (date == null){
    		return null;
    	}
    	cal.setTime(date);
    	return cal.get(Calendar.SECOND);
    }

    //getMillisecond
    class GetMillisecondFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			getMillisecondInit(context);			
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String timeZone = null;
			if (context.getParams().length > 1) {
				timeZone = stack.popString();
			}
			stack.push(getMillisecond(context, stack.popDate(), timeZone));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void getMillisecondInit(TLFunctionCallContext context) {
    	context.setCache(TLCalendarCache.withTimeZone(context, 1));
    }

    @TLFunctionAnnotation("Returns the millisecond within the second using the default time zone.")
    public static final Integer getMillisecond(TLFunctionCallContext context, Date date) {
    	return getMillisecond(context, date, null);
    }

    @TLFunctionAnnotation("Returns the millisecond within the second using the specified time zone.")
    public static final Integer getMillisecond(TLFunctionCallContext context, Date date, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 1);
    	if (date == null){
    		return null;
    	}
    	cal.setTime(date);
    	return cal.get(Calendar.MILLISECOND);
    }

    //createDate
    class CreateDateFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			createDateInit(context);			
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			Integer millisecond = 0;
			Integer second = 0;
			Integer minute = 0;
			Integer hour = 0;
			Integer day = null;
			Integer month = null;
			Integer year = null;
			String timeZone = null;
			
			int paramCount = context.getParams().length;
			
			// first deal with time zone, always the last parameter
			if (context.getParams()[paramCount-1].isString()
					|| (paramCount == 8) || (paramCount == 4)) { // not ambiguous even for null literal
				timeZone = stack.popString();
				paramCount--; // decrease the number of params
			}
			
			switch (paramCount) { // intentionally no breaks here!
			case 7:
				millisecond = stack.popInt();
			case 6:
				second = stack.popInt();
				minute = stack.popInt();
				hour = stack.popInt();
			case 3:
				day = stack.popInt();
				month = stack.popInt();
				year = stack.popInt();
			}
			
			stack.push(createDate(context, year, month, day, hour, minute, second, millisecond, timeZone));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void createDateInit(TLFunctionCallContext context) {
    	TLCache cache = null;
    	switch (context.getLiteralsSize()) {
    	case 3:	case 6:
    		cache = new TLCalendarCache(context);
    		break;
    	case 4:
    		cache = TLCalendarCache.withTimeZone(context, 3);
    		break;
    	case 7:
    		// the last parameter may be time zone or milliseconds
    		cache = TLCalendarCache.withTimeZone(context, 6);
    		break;
    	case 8:
    		cache = TLCalendarCache.withTimeZone(context, 7);
    		break;
    	}
    	context.setCache(cache);
    }
    
    private static final Date createDate(TLFunctionCallContext context, Integer year, Integer month, Integer day, Integer hour, Integer minute, Integer second, Integer millisecond, Calendar cal) {
    	cal.set(year, month-1, day, hour, minute, second);
    	cal.set(Calendar.MILLISECOND, millisecond);
    	return cal.getTime();
    }

    @TLFunctionAnnotation("Creates a new date without time using the default time zone. The first month has the value 1.")
    public static final Date createDate(TLFunctionCallContext context, Integer year, Integer month, Integer day) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCalendar();
    	return createDate(context, year, month, day, 0, 0, 0, 0, cal);
    }

    @TLFunctionAnnotation("Creates a new date without time using the specified time zone. The first month has the value 1.")
    public static final Date createDate(TLFunctionCallContext context, Integer year, Integer month, Integer day, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 3);
    	return createDate(context, year, month, day, 0, 0, 0, 0, cal);
    }

    @TLFunctionAnnotation("Creates a new date with time using the default time zone. The first month has the value 1.")
    public static final Date createDate(TLFunctionCallContext context, Integer year, Integer month, Integer day, Integer hour, Integer minute, Integer second) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCalendar();
    	return createDate(context, year, month, day, hour, minute, second, 0, cal);
    }

    @TLFunctionAnnotation("Creates a new date with time using the specified time zone. The first month has the value 1.")
    public static final Date createDate(TLFunctionCallContext context, Integer year, Integer month, Integer day, Integer hour, Integer minute, Integer second, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 6);
    	return createDate(context, year, month, day, hour, minute, second, 0, cal);
    }

    @TLFunctionAnnotation("Creates a new date with time using the default time zone. The first month has the value 1.")
    public static final Date createDate(TLFunctionCallContext context, Integer year, Integer month, Integer day, Integer hour, Integer minute, Integer second, Integer millisecond) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCalendar();
    	return createDate(context, year, month, day, hour, minute, second, millisecond, cal);
    }

    @TLFunctionAnnotation("Creates a new date with time using the specified time zone. The first month has the value 1.")
    public static final Date createDate(TLFunctionCallContext context, Integer year, Integer month, Integer day, Integer hour, Integer minute, Integer second, Integer millisecond, String timeZone) {
    	Calendar cal = ((TLCalendarCache) context.getCache()).getCachedCalendarWithTimeZone(context, timeZone, 7);
    	return createDate(context, year, month, day, hour, minute, second, millisecond, cal);
    }
}
