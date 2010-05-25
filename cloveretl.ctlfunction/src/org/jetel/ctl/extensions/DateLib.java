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

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.data.DateFieldEnum;
import org.jetel.util.DataGenerator;

public class DateLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "Date";
    
    private static Map<Thread, DataGenerator> dataGenerators = new HashMap<Thread, DataGenerator>();
    
    private static synchronized DataGenerator getGenerator(Thread key) {
    	DataGenerator generator = dataGenerators.get(key);
    	if (generator == null) {
    		generator = new DataGenerator();
    		dataGenerators.put(key, generator);
    	}
    	return generator;
    }
    
    @Override
    public TLFunctionPrototype getExecutable(String functionName) {
    	final TLFunctionPrototype ret = 
    		"datediff".equals(functionName) ? new DateDiffFunction() : 
    		"dateadd".equals(functionName) ? new DateAddFunction() :
    		"today".equals(functionName) ? new TodayFunction() :
    		"zero_date".equals(functionName) ? new ZeroDateFunction() :
    		"extract_date".equals(functionName) ? new ExtractDateFunction() :
    		"extract_time".equals(functionName) ? new ExtractTimeFunction() :
    		"trunc".equals(functionName) ? new TruncFunction() :
    	    "trunc_date".equals(functionName)? new TruncDateFunction() :
    	    "random_date".equals(functionName) ? new RandomDateFunction() : null;
    		
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
    
		return ret;
			
    }
    

    @TLFunctionAnnotation("Returns current date and time.")
    public static final Date today(TLFunctionCallContext context) {
    	return new Date();
    }

    // TODAY
    class TodayFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(today(context));
		}
    	
    }
        
    // DATEADD
	class DateAddFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			dateadd_init(context);
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final DateFieldEnum unit = (DateFieldEnum)stack.pop();
			final Integer shift = stack.popInt();
			final Date lhs = stack.popDate();
			
			stack.push(dateadd(context, lhs,shift,unit));
		}
	}
	
	@TLFunctionInitAnnotation
    public static final void dateadd_init(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache());
    }
    
    @TLFunctionAnnotation("Adds to a component of a date (e.g. month)")
    public static final Date dateadd(TLFunctionCallContext context, Date lhs, Integer shift, DateFieldEnum unit) {
    	Calendar c = ((TLCalendarCache)context.getCache()).getCalendar();
    	c.setTime(lhs);
    	c.add(unit.asCalendarField(),shift);
    	
        return c.getTime();

    }

	
	// DATEDIFF
    class DateDiffFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			datediff_init(context);
		}

    	public void execute(Stack stack, TLFunctionCallContext context) {
    		final DateFieldEnum unit = (DateFieldEnum)stack.pop();
    		final Date rhs = stack.popDate();
    		final Date lhs = stack.popDate();
    		
    		stack.push(datediff(context, lhs, rhs, unit));
    	}

	}
    
    @TLFunctionInitAnnotation
    public static final void datediff_init(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache());
    }
    
    @TLFunctionAnnotation("Returns the difference between dates")
    public static final Integer datediff(TLFunctionCallContext context, Date lhs, Date rhs, DateFieldEnum unit) {
		long diffSec = lhs.getTime() - rhs.getTime() / 1000;
        int diff = 0;
        
        Calendar cal = null;
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
	
    public static final Date zero_date() {
    	return new Date(0L);
    }
    
    class ZeroDateFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(zero_date());
		}
    }
    

    
    // extract_date
    // TODO: add test case
    class ExtractDateFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			extract_date_init(context);
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(extract_date(context, stack.popDate()));
		}
	}

    @TLFunctionInitAnnotation
    public static final void extract_date_init(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache());
    }
    
    @TLFunctionAnnotation("Extracts only date portion from date-time value, setting all time fields to zero.")
	public static final Date extract_date(TLFunctionCallContext context, Date d) {
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
    
    // extract_time
    // TODO: add test case
    class ExtractTimeFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			extract_time_init(context);
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(extract_time(context, stack.popDate()));
		}
	}

    @TLFunctionInitAnnotation
	public void extract_time_init(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache());
	}

    @TLFunctionAnnotation("Extracts only time portion from date-time value, clearing all date fields.")
	public static final Date extract_time(TLFunctionCallContext context, Date d) {
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

		public void init(TLFunctionCallContext context) {
			trunc_init(context);
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if(context.getParams()[0].isDecimal()) {
				stack.push(trunc(context, stack.popDecimal()));
			} else if (context.getParams()[0].isDouble()) {
				stack.push(trunc(context, stack.popDouble()));
			} else if (context.getParams()[0].isDate()) {
				stack.push(trunc(context, stack.popDate()));
			} else if (context.getParams()[0].isMap()) {
				stack.push(trunc(context, stack.popMap()));
			} else {
				stack.push(trunc(context, stack.popList()));
			}
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void trunc_init(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache());
    }
    
	@TLFunctionAnnotation("Truncates BigDecimal - returns long part of number, decimal part is discarded.")
    public static final Long trunc(TLFunctionCallContext context, BigDecimal value) {
    	return value.longValue();
    }
    
    @TLFunctionAnnotation("Truncates Double - returns long part of double, decimal part is discarded.")
    public static final Long trunc(TLFunctionCallContext context, Double value) {
    	return value.longValue();
    }
    
    @TLFunctionAnnotation("Returns date with the same year,month and day, but hour, minute, second and millisecond are set to zero values.")
    public static final Date trunc(TLFunctionCallContext context, Date value) {
    	Calendar cal = ((TLCalendarCache)context.getCache()).getCalendar();
    	cal.setTime(value);
    	cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE , 0);
        cal.set(Calendar.SECOND , 0);
        cal.set(Calendar.MILLISECOND , 0);
        value.setTime(cal.getTimeInMillis());
        return value;
    }
    
    @TLFunctionAnnotation("Emptyes the passed List and returns null.")
    public static final <E> List<E> trunc(TLFunctionCallContext context, List<E> value) {
    	value.clear();
    	return null;
    }
    
    @TLFunctionAnnotation("Emptyes the passed Map and returns null.")
    public static final <E, F> Map<E, F> trunc(TLFunctionCallContext context, Map<E, F> value) {
    	value.clear();
    	return null;
    }

    //Trunc date
    class TruncDateFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			trunc_date_init(context);			
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(trunc_date(context, stack.popDate()));
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void trunc_date_init(TLFunctionCallContext context) {
    	context.setCache(new TLCalendarCache());
    }

    @TLFunctionAnnotation("Returns the date with the same hour, minute, second and millisecond, but year, month and day are set to zero values.")
    public static final Date trunc_date(TLFunctionCallContext context, Date date) {
    	Calendar cal = ((TLCalendarCache)context.getCache()).getCalendar();
    	cal.setTime(date);
    	cal.set(Calendar.YEAR,0);
        cal.set(Calendar.MONTH,0);
        cal.set(Calendar.DAY_OF_MONTH,1);
        date.setTime(cal.getTimeInMillis());
        return date;
    }
    
    //Random date
    class RandomDateFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			random_date_init(context);
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			Long randomSeed = null;
			String locale = null;
			String format;
			if (context.getParams().length > 3) {
				if (context.getParams().length > 4) {
					randomSeed = stack.popLong();
				}
				if (context.getParams().length > 3) {
					if (context.getParams()[3].isLong()) {
						randomSeed = stack.popLong();
					} else {
						locale = stack.popString();
					}
				}
				format = stack.popString();
				String to = stack.popString();
				String from = stack.popString();
				if (randomSeed == null) {
					random_date(context, from, to, format, locale);
				} else {
					random_date(context, from, to, format, locale, randomSeed);
				}
				
			} else if (context.getParams().length > 2){
				if (context.getParams()[2].isLong()) {
					randomSeed = stack.popLong();
					if (context.getParams()[1].isDate()) {
						Date from = stack.popDate();
						Date to = stack.popDate();
						stack.push(random_date(context, from, to, randomSeed));
					} else {
						Long from = stack.popLong();
						Long to = stack.popLong();
						stack.push(random_date(context, from, to, randomSeed));
					}
				} else {
					format = stack.popString();
					String to = stack.popString();
					String from = stack.popString();
					stack.push(random_date(context, from, to, format));
				}
			} else {
				if (context.getParams()[1].isDate()) {
					Date to = stack.popDate();
					Date from = stack.popDate();
					stack.push(random_date(context, from, to));
				} else {
					Long from = stack.popLong();
					Long to = stack.popLong();
					stack.push(random_date(context, from, to));
				}
			}
		}
    }
    
    @TLFunctionInitAnnotation
    public static final void random_date_init(TLFunctionCallContext context) {
    	
    }

    @TLFunctionAnnotation("Generates a random date from interval specified by two dates.")
    public static final Date random_date(TLFunctionCallContext context, Date from, Date to) {
    	return random_date(context, from.getTime(), to.getTime());
    }
    
    @TLFunctionAnnotation("Generates a random date from interval specified by Long representation of dates. Allows changing seed.")
    public static final Date random_date(TLFunctionCallContext context, Long from, Long to) {
    	if (to > from) {
    		throw new TransformLangExecutorRuntimeException("random_date - fromDate is greater than toDate");
    	}
    	return new Date(getGenerator(Thread.currentThread()).nextLong(from, to));
    }
    
    @TLFunctionAnnotation("Generates a random date from interval specified by two dates. Allows changing seed.")
    public static final Date random_date(TLFunctionCallContext context, Date from, Date to, Long randomSeed) {
    	return random_date(context, from.getTime(), to.getTime(), randomSeed);
    }
    
    @TLFunctionAnnotation("Generates a random date from interval specified by Long representation of dates. Allows changing seed.")
    public static final Date random_date(TLFunctionCallContext context, Long from, Long to, Long randomSeed) {
    	if (to > from) {
    		throw new TransformLangExecutorRuntimeException("random_date - fromDate is greater than toDate");
    	}
    	DataGenerator generator = getGenerator(Thread.currentThread());
    	generator.setSeed(randomSeed);
    	return new Date(getGenerator(Thread.currentThread()).nextLong(from, to));
    }
    
    @TLFunctionAnnotation("Generates a random date from interval specified by string representation of dates in given format.")
    public static final Date random_date(TLFunctionCallContext context, String from, String to, String format) {
    	SimpleDateFormat sdf = new SimpleDateFormat(format);
    	return random_date(context, from, to, sdf);
    }
    
    @TLFunctionAnnotation("Generates a random from interval specified by string representation of dates in given format and locale.")
    public static final Date random_date(TLFunctionCallContext context, String from, String to, String format, String locale) {
    	SimpleDateFormat sdf = new SimpleDateFormat(format, parseLocale(locale));
    	return random_date(context, from, to, sdf);
    }
    
    @TLFunctionAnnotation("Generates a random date from interval specified by string representation of dates in given format. Allows changing seed.")
    public static final Date random_date(TLFunctionCallContext context, String from, String to, String format, Long randomSeed) {
    	SimpleDateFormat sdf = new SimpleDateFormat(format);
    	return random_date(context, from, to, randomSeed, sdf);
    }
    
    @TLFunctionAnnotation("Generates a random date from interval specified by string representation of dates in given format and locale. Allows changing seed.")
    public static final Date random_date(TLFunctionCallContext context, String from, String to, String format, String locale, Long randomSeed) {
    	SimpleDateFormat sdf = new SimpleDateFormat(format, parseLocale(locale));
    	return random_date(context, from, to, randomSeed, sdf);
    }
    
    private static final Locale parseLocale(String locale) {
    	String[] aLocale = locale.split("\\.");// '.' standard delimiter
		return aLocale.length < 2 ? new Locale(aLocale[0]) : new Locale(aLocale[0], aLocale[1]);
    }
    
    private static final Date random_date(TLFunctionCallContext context, String from, String to, SimpleDateFormat formatter) {
    	try {
			long fromTime = formatter.parse(from).getTime();
			long toTime = formatter.parse(to).getTime();
			return random_date(context, fromTime, toTime);
		} catch (ParseException e) {
			throw new TransformLangExecutorRuntimeException("random_date - " + e.getMessage());
		}
    }
    
    private static final Date random_date(TLFunctionCallContext context, String from, String to, Long randomSeed, SimpleDateFormat formatter) {
    	try {
			long fromTime = formatter.parse(from).getTime();
			long toTime = formatter.parse(to).getTime();
			return random_date(context, fromTime, toTime, randomSeed);
		} catch (ParseException e) {
			throw new TransformLangExecutorRuntimeException("random_date - " + e.getMessage());
		}
    }
    
}
