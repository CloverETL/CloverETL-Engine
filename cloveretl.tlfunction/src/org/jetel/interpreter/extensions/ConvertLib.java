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

import java.text.SimpleDateFormat;

import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLDateValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.interpreter.extensions.DateLib.CalendarStore;

public class ConvertLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "Convert";

    enum Function {
        NUM2STR("num2str"),
        DATE2STR("date2str"),
        STR2DATE("str2date"),
        DATE2NUM("date2num"),
        STR2NUM("str2num");
        
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

    public ConvertLib() {
        super();
     }

    public TLFunctionPrototype getFunction(String functionName) {
        switch(Function.fromString(functionName)) {
        case NUM2STR: return new Num2StrFunction();
        case DATE2STR: return new Date2StrFunction();
        case STR2DATE: return new Str2DateFunction();
        case DATE2NUM: return new Date2NumFunction();
        case STR2NUM: return new Str2NumFunction();
        default: return null;
       }
    }

    //  NUM2STR
    class Num2StrFunction extends TLFunctionPrototype {

        public Num2StrFunction() {
            super("string", "num2str", new TLValueType[] { TLValueType.DECIMAL, TLValueType.INTEGER }, 
                    TLValueType.STRING, 2,1);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            TLValue input=params[0];
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
            
            if (params[0]==TLValue.NULL_VAL) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.NUM2STR.name()+" - NULL value not allowed");
            }
            int radix=10;
            
            if (params.length>1) {
                if (params[1]==TLValue.NULL_VAL || !params[1].type.isNumeric() ){
                    throw new TransformLangExecutorRuntimeException(params,
                        Function.NUM2STR.name()+" - wrong type of literals");
                }
                radix=((TLNumericValue)params[1]).getInt();
            }
            
                if (radix == 10) {
                    strBuf.append(input.toString());
                } else {
                    switch(input.type) {
                    case INTEGER:
                        strBuf.append(Integer.toString(((TLNumericValue)input).getInt(),radix));
                        break;
                    case LONG:
                        strBuf.append(Long.toString(((TLNumericValue)input).getLong(),radix));
                        break;
                    case DOUBLE:
                        strBuf.append(Double.toHexString(((TLNumericValue)input).getDouble()));
                        break;
                    default:
                    throw new TransformLangExecutorRuntimeException(params,
                            Function.NUM2STR.name()+" - can't convert number to string using specified radix");
                    }
                }

            
            return val;
        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
    }

    
    
    //  DATE2STR
    class Date2StrFunction extends TLFunctionPrototype {
        
        class Date2StrContext {
            TLValue value;
            SimpleDateFormat format;
        }
        

        public Date2StrFunction() {
            super("string", "date2str", new TLValueType[] { TLValueType.DATE, TLValueType.STRING }, 
                    TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = ((Date2StrContext)context.getContext()).value;
            SimpleDateFormat format=((Date2StrContext)context.getContext()).format;
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
            
            if (params[0]==TLValue.NULL_VAL || params[1]==TLValue.NULL_VAL) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.DATE2STR.name()+" - NULL value not allowed");
            }
            
            if (params[0].type!=TLValueType.DATE || params[1].type!=TLValueType.STRING)
                throw new TransformLangExecutorRuntimeException(params,
                    "date2str - wrong type of literal");

            format.applyPattern(params[1].toString());
            strBuf.append(format.format(((TLDateValue)params[0]).getDate()));
            return val;
        }

        @Override
        public TLContext createContext() {
            Date2StrContext con=new Date2StrContext();
            con.value=TLValue.create(TLValueType.STRING);
            con.format=new SimpleDateFormat();

            TLContext<Date2StrContext> context=new TLContext<Date2StrContext>();
            context.setContext(con);
            
            return context;
        }
    }

    //  STR2DATE
    class Str2DateFunction extends TLFunctionPrototype {
        
        class Str2DateContext {
            TLValue value;
            SimpleDateFormat format;
        }
        

        public Str2DateFunction() {
            super("string", "str2date", new TLValueType[] { TLValueType.STRING, TLValueType.STRING }, 
                    TLValueType.DATE);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = ((Str2DateContext)context.getContext()).value;
            SimpleDateFormat format=((Str2DateContext)context.getContext()).format;
            
            
            if (params[0]==TLValue.NULL_VAL || params[1]==TLValue.NULL_VAL) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.STR2DATE.name()+" - NULL value not allowed");
            }
            
            if (params[0].type!=TLValueType.STRING || params[1].type!=TLValueType.STRING)
                throw new TransformLangExecutorRuntimeException(params,
                        Function.STR2DATE.name()+" - wrong type of literal");

            format.applyPattern(params[1].toString());
            try {
                ((TLDateValue)val).getDate().setTime(format.parse(params[0].toString()).getTime());
            }catch (java.text.ParseException ex) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.STR2DATE.name()+" - can't convert \"" + params[0] + "\" using format "+format.toPattern());
            }
            
            return val;
        }

        @Override
        public TLContext createContext() {
            Str2DateContext con=new Str2DateContext();
            con.value=TLValue.create(TLValueType.DATE);
            con.format=new SimpleDateFormat();

            TLContext<Str2DateContext> context=new TLContext<Str2DateContext>();
            context.setContext(con);
            
            return context;
        }
    }
    
    
    // 	DATE2NUM
    class Date2NumFunction extends TLFunctionPrototype {

        public Date2NumFunction() {
            super("string", "date2num", new TLValueType[] { TLValueType.DATE, TLValueType.SYM_CONST }, 
                    TLValueType.INTEGER);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	if (params[0].type!=TLValueType.DATE || params[1].type!=TLValueType.SYM_CONST){
                throw new TransformLangExecutorRuntimeException(params,
                        "date2num - wrong type of literals");
        	}
        	CalendarStore calStore=(CalendarStore) context.getContext();
        	
        	calStore.calStart.setTime(((TLDateValue)params[0]).getDate());
        	((TLNumericValue)calStore.value).getNumeric().setValue(calStore.calStart.get(TLFunctionUtils.astToken2CalendarField(params[1])));

        	return calStore.value;
        }


        @Override
		public TLContext createContext() {
			TLContext<CalendarStore> context = new TLContext<CalendarStore>();
			context.setContext(new CalendarStore(TLValueType.INTEGER));
			return context;
		}
    }
    
    
    
    // STR2NUM
    class Str2NumFunction extends TLFunctionPrototype {

        public Str2NumFunction() {
            super("string", "str2num", new TLValueType[] { TLValueType.STRING, TLValueType.SYM_CONST, TLValueType.INTEGER }, 
                    TLValueType.INTEGER,3,1);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	if ((params[0].type!=TLValueType.STRING) || 
        			(params.length>1 && params[1].type!=TLValueType.SYM_CONST) || 
        			(params.length>2 && !params[2].type.isNumeric()) ){
                throw new TransformLangExecutorRuntimeException(params,
                        "str2num - wrong type of literals");
        	}
        	TLValueType valType = (params.length>1 ? TLFunctionUtils.astToken2ValueType(params[1]) : TLValueType.INTEGER);
        	TLValue value=(TLValue)context.getContext();
        	if (value==null && !(valType == TLValueType.DECIMAL)){
        		// initialize
        		value=TLValue.create(valType);
        		context.setContext(value);
        	}
        	int radix=10;
        	if (params.length>2){
        		radix=((TLNumericValue)params[2]).getInt();
        	}
        	
        	try{
                switch (valType) {
                case INTEGER:
                	((TLNumericValue)value).setValue(Integer.parseInt(params[0].toString(), radix));
                    break;
                case LONG:
                	((TLNumericValue)value).setValue(Long.parseLong(params[0].toString(), radix));
                    break;
                case DECIMAL:
                    if (radix == 10) {
                    	Decimal d = DecimalFactory.getDecimal(params[0].toString());
                    	if (value != null) {
                    		((TLNumericValue)value).setValue(d);
                    	}else{
                    		value = new TLNumericValue(TLValueType.DECIMAL,d);
                    		context.setContext(value);                    		
                    	}
//                    	value.getNumeric().setValue (DecimalFactory.getDecimal(params[0].getString()));
                    } else {
                        throw new TransformLangExecutorRuntimeException(params,
                                "str2num - can't convert string to decimal number using specified radix");
                    }
                    break;
                case DOUBLE:
                	if (radix==10 || radix==16){
                		((TLNumericValue)value).setValue(Double.parseDouble(params[0].toString()));
                }else{
                        throw new TransformLangExecutorRuntimeException(params,
                                "str2num - can't convert string to number/double number using specified radix");
                    }
                }
            } catch (NumberFormatException ex) {
                throw new TransformLangExecutorRuntimeException(params, "str2num - can't convert \"" + params[0] + "\"");
            }
        return value;
    }
    
	@Override
	public TLContext createContext() {
		 TLContext<TLValue> context = new TLContext<TLValue>();
	        context.setContext(null);
	        return context;
	}
    }

}
