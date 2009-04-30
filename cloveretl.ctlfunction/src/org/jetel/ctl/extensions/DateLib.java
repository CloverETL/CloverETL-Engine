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
    		"dateadd".equals(functionName) ? new DateAddFunction() : null;
    				
    		
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
    
		return ret;
			
    }
    
//    enum Function {
//        TODAY("today"), DATEADD("dateadd"), DATEDIFF("datediff"), 
//        TRUNC("trunc"), TRUNC_DATE("trunc_date");
//        
//        public String name;
//        
//        private Function(String name) {
//            this.name = name;
//        }
//        
//        public static Function fromString(String s) {
//            for(Function function : Function.values()) {
//                if(s.equalsIgnoreCase(function.name) || s.equalsIgnoreCase(LIBRARY_NAME + "." + function.name)) {
//                    return function;
//                }
//            }
//            return null;
//        }
//    }
//
//    public DateLib() {
//        super();
//     }
//
//    public TLFunctionPrototype getFunction(String functionName) {
//        switch(Function.fromString(functionName)) {
//        case TODAY: return new TodayFunction();
//        case DATEADD: return new DateaddFunction();
//        case DATEDIFF: return new DatediffFunction();
//        case TRUNC: return new TruncFunction();
//        case TRUNC_DATE:return new TruncDateFunction();
//        default: return null;
//       }
//    }
//    
//    public  Collection<TLFunctionPrototype> getAllFunctions() {
//    	List<TLFunctionPrototype> ret = new ArrayList<TLFunctionPrototype>();
//    	Function[] fun = Function.values();
//    	for (Function function : fun) {
//    		ret.add(getFunction(function.name));
//		}
//    	
//    	return ret;
//    }
//
//    // TODAY
//    class TodayFunction extends TLFunctionPrototype {
//    	
//    	public TodayFunction(){
//    		super("date", "today", "Returns current date and time",
//    				new TLType[][] { }, TLTypePrimitive.DATETIME, 0, 0);
//    	}
//    
//        @Override
//        public TLValue execute(TLValue[] params, TLContext context) {
//            TLDateValue val=(TLDateValue)context.getContext();
//            val.getDate().setTime(Calendar.getInstance().getTimeInMillis()); 
//            return  val;
//        }
//        
//        @Override
//        public TLContext createContext() {
//            return TLContext.createDateContext();
//        }
//        
////        @Override
////        public TLType checkParameters(TLType[] parameters) {
////        	if (parameters.length > 0) {
////        		return TLType.ERROR;
////        	}
////        	
////        	return TLTypePrimitive.DATETIME;
////        }
//    }
//    
//    
    
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
	
	
//	// TRUNC
//	
//	class TruncFunction extends TLFunctionPrototype {
//
//		public TruncFunction() {
//			super("date", "trunc", "Truncates numeric types, dates or list variables", 
//					new TLType[][] { { TLTypePrimitive.LONG, TLTypePrimitive.DATETIME, TLType.createList(TLType.OBJECT), TLType.createMap(TLType.OBJECT,TLType.OBJECT) } 
//					}, TLType.OBJECT);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TruncStore store=(TruncStore)context.getContext();
//			TLValueType type=params[0].type;
//			if (store.value==null){
//				if (type==TLValueType.DATE) {
//					store.cal=Calendar.getInstance();
//					store.value=TLValue.create(TLValueType.DATE);
//				}else if (type.isNumeric()) {
//					store.value=TLValue.create(TLValueType.LONG);
//				}else if (type.isArray()){
//					store.value=TLNullValue.getInstance();
//				}
//			}
//
//			if (type==TLValueType.DATE ) {
//	            store.cal.setTime(((TLDateValue)params[0]).getDate());
//	            store.cal.set(Calendar.HOUR_OF_DAY, 0);
//	            store.cal.set(Calendar.MINUTE , 0);
//	            store.cal.set(Calendar.SECOND , 0);
//	            store.cal.set(Calendar.MILLISECOND , 0);
//	            
//	            ((TLDateValue)store.value).getDate().setTime(store.cal.getTimeInMillis());
//	        }else if (type.isNumeric()){
//	        	store.value.setValue(params[0]);
//	        }else if (type.isArray()) {
//	        	((TLContainerValue)params[0]).clear();
//	        }else {
//	            throw new TransformLangExecutorRuntimeException(params,
//	                    "trunc - wrong type of literal");
//	        }
//
//	        return store.value;
//	    }
//	
//			@Override
//			public TLContext createContext() {
//				 TLContext<TruncStore> context = new TLContext<TruncStore>();
//			        context.setContext(new TruncStore());
//			        return context;
//			}
//			
//			@Override
//			public TLType checkParameters(TLType[] parameters) {
//				if (parameters.length < minParams || parameters.length > maxParams) {
//					return TLType.ERROR;
//				}
//				
//				if (parameters[0].isNumeric()) {
//					return TLTypePrimitive.LONG;
//				}
//				
//				if (TLTypePrimitive.DATETIME.canAssign(parameters[0])) {
//					return TLTypePrimitive.DATETIME;
//				}
//				
//				if (parameters[0].isList()) {
//					return parameters[0];
//				}
//				
//				if (parameters[0].isMap()) {
//					return parameters[0];
//				}
//				
//				return TLType.ERROR;
//			}
//		}
//	
//// TRUNC
//	
//	class TruncDateFunction extends TLFunctionPrototype {
//
//		public TruncDateFunction() {
//			super("date", "trunc_date", "Truncates date portion of datetime", 
//					new TLType[][] { { TLTypePrimitive.DATETIME } }, TLTypePrimitive.DATETIME);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TruncStore store=(TruncStore)context.getContext();
//			if (store.value==null){
//					store.cal=Calendar.getInstance();
//					store.value=TLValue.create(TLValueType.DATE);
//			}
//
//			if (params[0].type==TLValueType.DATE ) {
//	            store.cal.setTime(((TLDateValue)params[0]).getDate());
//	            store.cal.set(Calendar.YEAR,0);
//	            store.cal.set(Calendar.MONTH,0);
//	            store.cal.set(Calendar.DAY_OF_MONTH,1);
//	            
//	            ((TLDateValue)store.value).getDate().setTime(store.cal.getTimeInMillis());
//	        }else {
//	            throw new TransformLangExecutorRuntimeException(params,
//	                    "trunc - wrong type of literal");
//	        }
//
//	        return store.value;
//	    }
//	
//			@Override
//			public TLContext createContext() {
//				 TLContext<TruncStore> context = new TLContext<TruncStore>();
//			        context.setContext(new TruncStore());
//			        return context;
//			}
//			
////			@Override
////			public TLType checkParameters(TLType[] parameters) {
////				if (parameters.length < minParams || parameters.length > maxParams) {
////					return TLType.ERROR;
////				}
////				
////				if (parameters[0] != TLTypePrimitive.DATETIME) {
////					return TLType.ERROR;
////				}
////				
////				return TLTypePrimitive.DATETIME;
////			}
//		}
//	
//	
//	
//	
//	/*General data structures*/
//	
//	public static class CalendarStore {
//		Calendar calStart;
//		Calendar calEnd;
//		TLValue value;
//
//		public CalendarStore(TLValueType valType) {
//			calStart = Calendar.getInstance();
//			calEnd = Calendar.getInstance();
//			value = TLValue.create(valType);
//		}
//	
//	}
//	
//	public static class TruncStore {
//		Calendar cal;
//		TLValue value;
//	}

}
