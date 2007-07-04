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
package org.jetel.interpreter.extensions;

import java.util.Calendar;

import org.jetel.data.primitive.CloverLong;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.ASTnode.CLVFTruncNode;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;

public class DateLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "Date";

    enum Function {
        TODAY("today"), DATEADD("dateadd"), DATEDIFF("datediff"), TRUNC("trunc");
        
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

    public TLFunctionPrototype getFunction(String functionName) {
        switch(Function.fromString(functionName)) {
        case TODAY: return new TodayFunction();
        case DATEADD: return new DateaddFunction();
        case DATEDIFF: return new DatediffFunction();
        case TRUNC: return new TruncFunction();
        default: return null;
       }
    }

    // TODAY
    class TodayFunction extends TLFunctionPrototype {
    	
    	public TodayFunction(){
    		super("date", "today", new TLValueType[] { }, TLValueType.DATE);
    	}
    
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val=(TLValue)context.getContext();
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
			super("date", "dateadd", new TLValueType[] { TLValueType.DATE,
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

			calStore.calStart.setTime(params[0].getDate());
			calStore.calStart.add(TLFunctionUtils.astToken2CalendarField(params[2]),params[1].getInt());
			calStore.value.getDate().setTime(calStore.calStart.getTimeInMillis());
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
			super("date", "datediff", new TLValueType[] { TLValueType.DATE,
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

			long diffSec = (params[0].getDate().getTime() - params[1].getDate().getTime()) / 1000;
            int diff = 0;
            int calendarField=TLFunctionUtils.astToken2CalendarField(params[2]);
            switch (calendarField) {
            case Calendar.SECOND:
                // we have the difference in seconds
                diff = (int) diffSec;
                break;
            case Calendar.MINUTE:
                // how many minutes'
                diff = (int) diffSec / 60;
                break;
            case Calendar.HOUR_OF_DAY:
                diff = (int) diffSec / 3600;
                break;
            case Calendar.DAY_OF_MONTH:
                // how many days is the difference
                diff = (int) diffSec / 86400;
                break;
            case Calendar.WEEK_OF_YEAR:
                // how many weeks
                diff = (int) diffSec / 604800;
                break;
            case Calendar.MONTH:
                calStore.calStart.setTime(params[0].getDate());
                calStore.calEnd.setTime(params[1].getDate());
                diff = ( calStore.calStart.get(Calendar.MONTH) + calStore.calStart
                        .get(Calendar.YEAR) * 12)
                        - (calStore.calEnd.get(Calendar.MONTH) + calStore.calEnd
                                .get(Calendar.YEAR) * 12);
                break;
            case Calendar.YEAR:
            	calStore.calStart.setTime(params[0].getDate());
                calStore.calEnd.setTime(params[1].getDate());
                diff = calStore.calStart.get(calendarField)
                        - calStore.calEnd.get(calendarField);
                break;
            default:
                Object arguments[] = { new Integer(params[2].getInt()) };
                throw new TransformLangExecutorRuntimeException(arguments,
                        "datediff - wrong difference unit");
			
            }
            calStore.value.getNumeric().setValue(diff);
                
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
			super("date", "trunc", new TLValueType[] { TLValueType.OBJECT }, TLValueType.OBJECT);
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
				}else{
					throw new TransformLangExecutorRuntimeException(params,
                    "trunc - wrong type of literal(s)");
				}
			}

			if (type==TLValueType.DATE ) {
	            store.cal.setTime(params[0].getDate());
	            store.cal.set(Calendar.HOUR_OF_DAY, 0);
	            store.cal.set(Calendar.MINUTE , 0);
	            store.cal.set(Calendar.SECOND , 0);
	            store.cal.set(Calendar.MILLISECOND , 0);
	            
	            store.value.getDate().setTime(store.cal.getTimeInMillis());
	        }else if (type.isNumeric()){
	        	store.value.getNumeric().setValue(params[0].getNumeric());
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

}
