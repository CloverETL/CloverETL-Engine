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
package org.jetel.interpreter.extensions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLContainerValue;
import org.jetel.interpreter.data.TLDateValue;
import org.jetel.interpreter.data.TLNullValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.util.DataGenerator;

public class DateLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "Date";

    enum Function {
        TODAY("today"), DATEADD("dateadd"), DATEDIFF("datediff"), TRUNC("trunc"), TRUNC_DATE("trunc_date"), 
        RANDOM_DATE("random_date");
        
        public String name;
        
        private Function(String name) {
            this.name = name;
        }
        
        public static Function fromString(String s) {
            for(Function function : Function.values()) {
                if(s.equalsIgnoreCase(function.name) || s.equalsIgnoreCase(LIBRARY_NAME + "." + function.name)) {
                    return function;
                }
            }
            return null;
        }
    }

    public DateLib() {
        super();
     }

    @Override
	public TLFunctionPrototype getFunction(String functionName) {
        switch(Function.fromString(functionName)) {
        case TODAY: return new TodayFunction();
        case DATEADD: return new DateaddFunction();
        case DATEDIFF: return new DatediffFunction();
        case TRUNC: return new TruncFunction();
        case TRUNC_DATE:return new TruncDateFunction();
        case RANDOM_DATE: return new RandomDateFunction();
        default: return null;
       }
    }
    
    @Override
	public  Collection<TLFunctionPrototype> getAllFunctions() {
    	List<TLFunctionPrototype> ret = new ArrayList<TLFunctionPrototype>();
    	Function[] fun = Function.values();
    	for (Function function : fun) {
    		ret.add(getFunction(function.name));
		}
    	
    	return ret;
    }

    // TODAY
    class TodayFunction extends TLFunctionPrototype {
    	
    	public TodayFunction(){
    		super("date", "today", "Returns current date and time",
    				new TLValueType[] { }, TLValueType.DATE);
    	}
    
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLDateValue val=(TLDateValue)context.getContext();
            val.getDate().setTime(Calendar.getInstance().getTimeInMillis()); 
            return  val;
        }
        
        @Override
        public TLContext createContext() {
            return TLContext.createDateContext();
        }
    }
    
    
    // DATEADD
	class DateaddFunction extends TLFunctionPrototype {

		public DateaddFunction() {
			super("date", "dateadd", "Adds to a component of a date (i.e. month)",
					new TLValueType[] { TLValueType.DATE,
					TLValueType.LONG, TLValueType.SYM_CONST }, TLValueType.DATE);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			CalendarStore calStore = (CalendarStore) context.getContext();
			if (params[0].type != TLValueType.DATE
					|| !params[1].type.isNumeric()
					|| params[2].type != TLValueType.SYM_CONST) {
				throw new TransformLangExecutorRuntimeException(params,
						"dateadd - wrong type of literal(s)");
			}

			calStore.calStart.setTime(((TLDateValue)params[0]).getDate());
			calStore.calStart.add(TLFunctionUtils.astToken2CalendarField(params[2]),((TLNumericValue)params[1]).getInt());
			((TLDateValue)calStore.value).getDate().setTime(calStore.calStart.getTimeInMillis());
			return calStore.value;
		}

		@Override
		public TLContext createContext() {
			TLContext<CalendarStore> context = new TLContext<CalendarStore>();
			context.setContext(new CalendarStore(TLValueType.DATE));
			return context;
		}

	}
	
	// DATEDIFF
	
	class DatediffFunction extends TLFunctionPrototype {

		public DatediffFunction() {
			super("date", "datediff", "Returns the difference between dates", new TLValueType[] { TLValueType.DATE,
					TLValueType.DATE, TLValueType.SYM_CONST }, TLValueType.INTEGER);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			CalendarStore calStore = (CalendarStore) context.getContext();
			if (params[0].type != TLValueType.DATE
					|| params[1].type != TLValueType.DATE
					|| params[2].type != TLValueType.SYM_CONST) {
				throw new TransformLangExecutorRuntimeException(params,
						"datediff - wrong type of literal(s)");
			}

			long diffSec = (((TLDateValue)params[0]).getDate().getTime() - ((TLDateValue)params[1]).getDate().getTime()) / 1000;
            int diff = 0;
            int calendarField=TLFunctionUtils.astToken2CalendarField(params[2]);
            switch (calendarField) {
            case Calendar.SECOND:
                // we have the difference in seconds
                diff = (int) diffSec;
                break;
            case Calendar.MINUTE:
                // how many minutes'
                diff = (int) (diffSec / 60L);
                break;
            case Calendar.HOUR_OF_DAY:
                diff = (int) (diffSec / 3600L);
                break;
            case Calendar.DAY_OF_MONTH:
                // how many days is the difference
                diff = (int) (diffSec / 86400L);
                break;
            case Calendar.WEEK_OF_YEAR:
                // how many weeks
                diff = (int) (diffSec / 604800L);
                break;
            case Calendar.MONTH:
                calStore.calStart.setTime(((TLDateValue)params[0]).getDate());
                calStore.calEnd.setTime(((TLDateValue)params[1]).getDate());
                diff = ( calStore.calStart.get(Calendar.MONTH) + calStore.calStart
                        .get(Calendar.YEAR) * 12)
                        - (calStore.calEnd.get(Calendar.MONTH) + calStore.calEnd
                                .get(Calendar.YEAR) * 12);
                break;
            case Calendar.YEAR:
            	calStore.calStart.setTime(((TLDateValue)params[0]).getDate());
                calStore.calEnd.setTime(((TLDateValue)params[1]).getDate());
                diff = calStore.calStart.get(calendarField)
                        - calStore.calEnd.get(calendarField);
                break;
            default:
                Object arguments[] = { new Integer(((TLNumericValue)params[2]).getInt()) };
                throw new TransformLangExecutorRuntimeException(arguments,
                        "datediff - wrong difference unit");
			
            }
            ((TLNumericValue)calStore.value).setValue(diff);
                
			return calStore.value;
		}

		@Override
		public TLContext createContext() {
			TLContext<CalendarStore> context = new TLContext<CalendarStore>();
			context.setContext(new CalendarStore(TLValueType.INTEGER));
			return context;
		}

	}
	
	
	// TRUNC
	
	class TruncFunction extends TLFunctionPrototype {

		public TruncFunction() {
			super("date", "trunc", "Truncates", new TLValueType[] { TLValueType.OBJECT }, TLValueType.OBJECT);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TruncStore store=(TruncStore)context.getContext();
			TLValueType type=params[0].type;
			if (store.value==null){
				if (type==TLValueType.DATE) {
					store.cal=Calendar.getInstance();
					store.value=TLValue.create(TLValueType.DATE);
				}else if (type.isNumeric()) {
					store.value=TLValue.create(TLValueType.LONG);
				}else if (type.isArray()){
					store.value=TLNullValue.getInstance();
				}
			}

			if (type==TLValueType.DATE ) {
	            store.cal.setTime(((TLDateValue)params[0]).getDate());
	            store.cal.set(Calendar.HOUR_OF_DAY, 0);
	            store.cal.set(Calendar.MINUTE , 0);
	            store.cal.set(Calendar.SECOND , 0);
	            store.cal.set(Calendar.MILLISECOND , 0);
	            
	            ((TLDateValue)store.value).getDate().setTime(store.cal.getTimeInMillis());
	        }else if (type.isNumeric()){
	        	store.value.setValue(params[0]);
	        }else if (type.isArray()) {
	        	((TLContainerValue)params[0]).clear();
	        }else {
	            throw new TransformLangExecutorRuntimeException(params,
	                    "trunc - wrong type of literal");
	        }

	        return store.value;
	    }
	
			@Override
			public TLContext createContext() {
				 TLContext<TruncStore> context = new TLContext<TruncStore>();
			        context.setContext(new TruncStore());
			        return context;
			}
		}
	
// TRUNC
	
	class TruncDateFunction extends TLFunctionPrototype {

		public TruncDateFunction() {
			super("date", "trunc_date", "Truncates date portion of datetime", new TLValueType[] { TLValueType.DATE }, TLValueType.DATE);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TruncStore store=(TruncStore)context.getContext();
			if (store.value==null){
					store.cal=Calendar.getInstance();
					store.value=TLValue.create(TLValueType.DATE);
			}

			if (params[0].type==TLValueType.DATE ) {
	            store.cal.setTime(((TLDateValue)params[0]).getDate());
	            store.cal.set(Calendar.YEAR,1970);
	            store.cal.set(Calendar.MONTH,0);
	            store.cal.set(Calendar.DAY_OF_MONTH,1);
	            
	            ((TLDateValue)store.value).getDate().setTime(store.cal.getTimeInMillis());
	        }else {
	            throw new TransformLangExecutorRuntimeException(params,
	                    "trunc - wrong type of literal");
	        }

	        return store.value;
	    }
	
			@Override
			public TLContext createContext() {
				 TLContext<TruncStore> context = new TLContext<TruncStore>();
			        context.setContext(new TruncStore());
			        return context;
			}
		}
	
	
// RANDOM_DATE
	
    class RandomDateFunction extends TLFunctionPrototype {

        public RandomDateFunction() {
            super("date", "random_date", "Generates a random date", 
           		 new TLValueType[] { TLValueType.OBJECT, TLValueType.OBJECT, TLValueType.OBJECT, TLValueType.OBJECT, TLValueType.LONG}, 
           		 TLValueType.DATE, 5, 2);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	RandomDateContext randomDateContext=(RandomDateContext)context.getContext();
        	DataGenerator dataGenerator = randomDateContext.getDataGenerator();

        	randomDateContext.checkAndSetParams(params);
        	if (randomDateContext.dirtyRandomSeed) {
        		dataGenerator.setSeed(randomDateContext.randomSeed);
        		randomDateContext.dirtyRandomSeed = false;
        	}
			
			// generate date
			randomDateContext.setValue(new Date(dataGenerator.nextLong(randomDateContext.fromDate, randomDateContext.toDate)));
	        return randomDateContext.value;
        }

        @Override
        public TLContext createContext() {
	        return RandomDateContext.createContex();        	
        }
    }
	

	/*General data structures*/
	
	public static class CalendarStore {
		Calendar calStart;
		Calendar calEnd;
		TLValue value;

		public CalendarStore(TLValueType valType) {
			calStart = Calendar.getInstance();
			calEnd = Calendar.getInstance();
			value = TLValue.create(valType);
		}
	
	}
	
	public static class TruncStore {
		Calendar cal;
		TLValue value;
	}

	private static class RandomDateContext {
		
        private DataGenerator dataGenerator = new DataGenerator();

		private TLValue value;
		private long fromDate;
		private long toDate;

		private CharSequence sFormat;
		private SimpleDateFormat format;
		private CharSequence sLocale;
		private Locale locale;
		private TLParameterCache paramsCache;
	    
		private long randomSeed = Long.MIN_VALUE;
		private boolean dirtyRandomSeed;
		
		public DataGenerator getDataGenerator() {
			return dataGenerator;
		}
		
	    public static TLContext createContex(){
	    	RandomDateContext con=new RandomDateContext();
	        con.value=TLValue.create(TLValueType.DATE);
	        con.format=new SimpleDateFormat();

	        TLContext<RandomDateContext> context=new TLContext<RandomDateContext>();
	        context.setContext(con);
	        return context;        	
	    }
	    
	    public void setValue(Date date) {
	    	value.setValue(date);
		}

	    /**
	     * Checks and sets all parameters. Creates fromDate and toDate values.
	     * @param params
	     * 		param[0] - String or Date - start date
	     * 		param[1] - String or Date - start date
	     * 		param[2] - optional - String - format || optional - int - randomSeed
	     * 		param[3] - optional - String - locale || optional - int - randomSeed
	     *  	param[4] - optional - int - randomSeed
	     */
		public void checkAndSetParams(TLValue[] params) {
			if (paramsCache==null){
				paramsCache=new TLParameterCache(params);
			}else{
				if (!paramsCache.hasChanged(params)) 
					return;
				else
					paramsCache.cache(params);
				
			}
			if (params[0].type == TLValueType.LONG) {
				fromDate = params[0].getNumeric().getLong();
			} else if (params[0].type == TLValueType.DATE) {
				fromDate = params[0].getDate().getTime();
			} else if (params[0].type == TLValueType.STRING) {
	        	// prepare the locale
				parseLocaleOrRandomSeed(params);

	        	// prepare the date formatter
	        	parseDataFormaterOrRandomSeed(params);
				
	    		try {
					fromDate = format.parse(((TLStringValue)params[0]).getCharSequence().toString()).getTime();
				} catch (ParseException e) {
		            throw new TransformLangExecutorRuntimeException(params, "random_date", e);
				}
			} else {
	            throw new TransformLangExecutorRuntimeException(params, "random_date - wrong type of first literal");
			}
			
			if (params[1].type == TLValueType.LONG) {
				toDate = params[1].getNumeric().getLong();
				if (params.length == 3) {
					// parse random seed
					parseRandomSeed(params[2], params);
				}
			} else if (params[1].type == TLValueType.DATE) {
				toDate = params[1].getDate().getTime();
				if (params.length == 3) {
					// parse random seed
					parseRandomSeed(params[2], params);
				}
			} else if (params[1].type == TLValueType.STRING) {
				if (params[0].type != TLValueType.STRING) {
		        	// prepare the locale
					parseLocaleOrRandomSeed(params);

		        	// prepare the date formatter
		        	parseDataFormaterOrRandomSeed(params);
				}
	    		try {
					toDate = format.parse(((TLStringValue)params[1]).getCharSequence().toString()).getTime();
				} catch (ParseException e) {
		            throw new TransformLangExecutorRuntimeException(params, "random_date", e);
				}
	        } else {
	            throw new TransformLangExecutorRuntimeException(params, "random_date - wrong type of second literal");
	        }
			if (toDate < fromDate) {
	            throw new TransformLangExecutorRuntimeException(params, "random_date - fromDate is greater than toDate");
			}
		}

		private final void parseLocaleOrRandomSeed(TLValue[] params) {
	    	// prepare the locale
			if (params.length >= 4) {
				if (params[3].type != TLValueType.STRING) {
					// parse random seed
					parseRandomSeed(params[3], params);
					return;
				}
				CharSequence sLocale = ((TLStringValue)params[3]).getCharSequence();
				if (sLocale.equals(this.sLocale)) return;
				this.sLocale = sLocale;
				String[] aLocale = ((TLStringValue)params[3]).getCharSequence().toString().split("\\.");// '.' standard delimiter
				if (aLocale.length < 2) locale = new Locale(aLocale[0]);
				else locale = new Locale(aLocale[0], aLocale[1]);
			}
			if (params.length == 5) {
				if (params[4].type != TLValueType.INTEGER) {
		            throw new TransformLangExecutorRuntimeException(params, "random_date - wrong locale type");
				}
				// parse random seed
				parseRandomSeed(params[4], params);
			}
	    }
	    
	    private final void parseDataFormaterOrRandomSeed(TLValue[] params) {
	    	// prepare the date formatter
	    	if (params.length > 2) {
				if (params[2].type != TLValueType.STRING) {
					// parse random seed
					parseRandomSeed(params[2], params);
					return;
				}
				CharSequence sFormat = ((TLStringValue)params[2]).getCharSequence();
				if (sFormat.equals(this.sFormat)) return;
				this.sFormat = sFormat;
				if (format == null) {
					format = locale == null ? new SimpleDateFormat(sFormat.toString()) : new SimpleDateFormat(sFormat.toString(), locale);
				} else if (locale == null) {
					format.applyPattern(sFormat.toString());
				} else {
					format = new SimpleDateFormat(sFormat.toString(), locale);
				}
		    	format.setLenient(false);
	    	} else {
	    		if (sLocale == null && sFormat == null && format != null) return;
	    		sLocale = null;
	    		sFormat = null;
	    		format = new SimpleDateFormat();
		    	format.setLenient(false);
	    	}
	    }

		private void parseRandomSeed(TLValue param, TLValue[] params) {
			if (param.type != TLValueType.INTEGER && param.type != TLValueType.LONG) {
	            throw new TransformLangExecutorRuntimeException(params, "random_date - wrong locale type");
			}
			// parse random seed
			if (randomSeed != ((TLNumericValue<?>)param).getLong()) {
				randomSeed = ((TLNumericValue<?>)param).getLong();
				dirtyRandomSeed = true;
			}
		}
	}
}


