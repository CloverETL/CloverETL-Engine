/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-07  David Pavlis <david.pavlis@centrum.cz> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 2.4.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.ctl.extensions;

import java.util.Calendar;
import java.util.Date;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.data.DateFieldEnum;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.extensions.TLFunctionAnnotation;
import org.jetel.ctl.extensions.TLFunctionLibrary;
import org.jetel.ctl.extensions.TLFunctionPrototype;


public class DateLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "Date";

    
    @Override
    public TLFunctionPrototype getExecutable(String functionName) {
    	final TLFunctionPrototype ret = 
    		"datediff".equals(functionName) ? new DateDiffFunction() : 
    		"dateadd".equals(functionName) ? new DateAddFunction() :
    		"today".equals(functionName) ? new TodayFunction() :
    		"zero_date".equals(functionName) ? new ZeroDateFunction() :
    		"extract_date".equals(functionName) ? new ExtractDateFunction() :
    		"extract_time".equals(functionName) ? new ExtractTimeFunction() : 
    		null;
    				
    		
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
    
		return ret;
			
    }
    

    @TLFunctionAnnotation("Returns current date and time.")
    public static final Date today() {
    	return new Date();
    }

    // TODAY
    class TodayFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(today());
		}
    	
    }
    
    @TLFunctionAnnotation("Adds to a component of a date (i.e. month)")
    public static final Date dateadd(Date lhs, Integer shift, DateFieldEnum unit) {
    	Calendar c = Calendar.getInstance();
    	c.setTime(lhs);
    	c.add(unit.asCalendarField(),shift);
    	
        return c.getTime();

    }
    
    // DATEADD
	class DateAddFunction implements TLFunctionPrototype {
		
		public void execute(Stack stack, TLType[] actualParams) {
			final DateFieldEnum unit = (DateFieldEnum)stack.pop();
			final Integer shift = stack.popInt();
			final Date lhs = stack.popDate();
			
			stack.push(dateadd(lhs,shift,unit));
		}

	}
	
	// DATEDIFF
    @TLFunctionAnnotation("Returns the difference between dates")
    public static final Integer datediff(Date lhs, Date rhs, DateFieldEnum unit) {
		long diffSec = lhs.getTime() - rhs.getTime() / 1000;
        int diff = 0;
        
        Calendar start = null;
        Calendar end = null;
        switch (unit) {
        case SECOND:
            // we have the difference in seconds
            diff = (int) diffSec;
            break;
        case MINUTE:
            // how many minutes'
            diff = (int) (diffSec / 60L);
            break;
        case HOUR:
            diff = (int) (diffSec / 3600L);
            break;
        case DAY:
            // how many days is the difference
            diff = (int) (diffSec / 86400L);
            break;
        case WEEK:
            // how many weeks
            diff = (int) (diffSec / 604800L);
            break;
        case MONTH:
        	start = Calendar.getInstance();
            start.setTime(lhs);
            end = Calendar.getInstance();
            end.setTime(rhs);
            diff = ( start.get(Calendar.MONTH) + start.get(Calendar.YEAR) * 12)
                    - (end.get(Calendar.MONTH) + end.get(Calendar.YEAR) * 12);
            break;
        case YEAR:
        	start = Calendar.getInstance();
        	start.setTime(lhs);
        	end = Calendar.getInstance();
            end.setTime(rhs);
            diff = start.get(Calendar.YEAR) - end.get(Calendar.YEAR);
            break;
        default:
            throw new TransformLangExecutorRuntimeException("Unknown time unit " + unit);
        }
        
        return diff;

    }

    class DateDiffFunction implements TLFunctionPrototype {

    	public void execute(Stack stack, TLType[] actualParams) {
    		final DateFieldEnum unit = (DateFieldEnum)stack.pop();
    		final Date rhs = stack.popDate();
    		final Date lhs = stack.popDate();
    		
    		stack.push(datediff(lhs, rhs, unit));
    	}

	}
	
    public static final Date zero_date() {
    	return new Date(0L);
    }
    
    class ZeroDateFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(zero_date());
		}
    }
    

    
    // extract_date
    @TLFunctionAnnotation("Extracts only date portion from date-time value, setting all time fields to zero.")
	public static final Date extract_date(Date d) {
    	// this hardcore code is necessary, subtracting milliseconds 
    	// or using Calendar.clear() does not seem to handle light-saving correctly
    	Calendar orig = Calendar.getInstance();
    	orig.setTime(d);
    	Calendar ret = Calendar.getInstance();
    	ret.clear();
    	for (int field  : new int[]{Calendar.DAY_OF_MONTH,Calendar.MONTH, Calendar.YEAR}) {
    		ret.set(field, orig.get(field));
    	}
    	return ret.getTime();
    }
    
    // TODO: add test case
    class ExtractDateFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(extract_date(stack.popDate()));
		}
	}

    // extract_time
    @TLFunctionAnnotation("Extracts only time portion from date-time value, clearing all date fields.")
	public static final Date extract_time(Date d) {
    	// this hardcore code is necessary, subtracting milliseconds 
    	// or using Calendar.clear() does not seem to handle light-saving correctly
    	Calendar orig = Calendar.getInstance();
    	orig.setTime(d);
    	Calendar ret = Calendar.getInstance();
    	ret.clear();
    	for (int field  : new int[]{Calendar.HOUR_OF_DAY,Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND}) {
    		ret.set(field, orig.get(field));
    	}
    	return ret.getTime();
    }
    
    // TODO: add test case
    class ExtractTimeFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(extract_time(stack.popDate()));
		}
	}

}
