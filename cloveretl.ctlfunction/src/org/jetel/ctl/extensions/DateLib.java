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
    	    "truncDate".equals(functionName)? new TruncDateFunction() : null;
    		
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
    	context.setCache(new TLCalendarCache());
    }
    
    @TLFunctionAnnotation("Adds to a component of a date (e.g. month)")
    public static final Date dateAdd(TLFunctionCallContext context, Date lhs, Long shift, DateFieldEnum unit) {
    	Calendar c = ((TLCalendarCache)context.getCache()).getCalendar();
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
    	context.setCache(new TLCalendarCache());
    }
    
    @TLFunctionAnnotation("Returns the difference between dates")
    public static final Long dateDiff(TLFunctionCallContext context, Date lhs, Date rhs, DateFieldEnum unit) {
    	if (unit == DateFieldEnum.MILLISEC) { // CL-1087
    		return lhs.getTime() - rhs.getTime();
    	}
		long diffSec = (lhs.getTime() - rhs.getTime()) / 1000L;
        long diff = 0;
        
        Calendar cal = null;
        switch (unit) {
        case SECOND:
            // we have the difference in seconds
            diff = diffSec;
            break;
        case MINUTE:
            // how many minutes'
            diff = diffSec / 60L;
            break;
        case HOUR:
            diff = diffSec / 3600L;
            break;
        case DAY:
            // how many days is the difference
            diff = diffSec / 86400L;
            break;
        case WEEK:
            // how many weeks
            diff = diffSec / 604800L;
            break;
        case MONTH:
        	cal = ((TLCalendarCache)context.getCache()).getCalendar();
            cal.setTime(lhs);
            diff = cal.get(Calendar.MONTH) + cal.get(Calendar.YEAR) * 12; 
            cal.setTime(rhs);
            diff -= cal.get(Calendar.MONTH) + cal.get(Calendar.YEAR) * 12;
            break;
        case YEAR:
        	cal = ((TLCalendarCache)context.getCache()).getCalendar();
        	cal.setTime(lhs);
        	diff = cal.get(Calendar.YEAR);
        	cal.setTime(rhs);
            diff -= cal.get(Calendar.YEAR);
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
    	context.setCache(new TLCalendarCache());
    }
    
    @TLFunctionAnnotation("Extracts only date portion from date-time value, setting all time fields to zero.")
	public static final Date extractDate(TLFunctionCallContext context, Date d) {
    	// this hardcore code is necessary, subtracting milliseconds 
    	// or using Calendar.clear() does not seem to handle light-saving correctly
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
    	context.setCache(new TLCalendarCache());
	}

    @TLFunctionAnnotation("Extracts only time portion from date-time value, clearing all date fields.")
	public static final Date extractTime(TLFunctionCallContext context, Date d) {
    	// this hardcore code is necessary, subtracting milliseconds 
    	// or using Calendar.clear() does not seem to handle light-saving correctly
    	Calendar cal = ((TLCalendarCache)context.getCache()).getCalendar();
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
    	context.setCache(new TLCalendarCache());
    }
    
    @TLFunctionAnnotation("Truncates other, but date-time values to zero.")
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
    	context.setCache(new TLCalendarCache());
    }

    @TLFunctionAnnotation("Truncates other, but day time values to zero.")
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
}
