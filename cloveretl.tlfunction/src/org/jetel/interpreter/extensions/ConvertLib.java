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

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.data.primitive.NumericFormat;
import org.jetel.data.primitive.StringFormat;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLDateValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.interpreter.extensions.DateLib.CalendarStore;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.MiscUtils;
import org.jetel.util.string.StringUtils;

public class ConvertLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "Convert";
	private static final String DEFAULT_REGEXP_TRUE_STRING = "T|TRUE|YES|Y||t|true|1|yes|y";
	private static final String DEFAULT_REGEXP_FALSE_STRING = "F|FALSE|NO|N||f|false|0|no|n";

	private static StringFormat trueFormat = StringFormat.create(DEFAULT_REGEXP_TRUE_STRING);
	private static StringFormat falseFormat = StringFormat.create(DEFAULT_REGEXP_FALSE_STRING);

	enum Function {
        NUM2STR("num2str"),
        DATE2STR("date2str"),
        STR2DATE("str2date"),
        DATE2NUM("date2num"),
        STR2NUM("str2num"),
        TRY_CONVERT("try_convert");
        
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
        case TRY_CONVERT: return new TryConvertFunction();
        default: return null;
       }
    }

    //  NUM2STR
    class Num2StrFunction extends TLFunctionPrototype {

        public Num2StrFunction() {
            super("convert", "num2str", new TLValueType[] { TLValueType.DECIMAL, TLValueType.INTEGER }, 
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
            super("convert", "date2str", new TLValueType[] { TLValueType.DATE, TLValueType.STRING }, 
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
       	 SimpleDateFormat formatter;
    	 ParsePosition position;
    	 String locale;
    	 
    	 public void init(String locale, String pattern){
    		 formatter = (SimpleDateFormat)MiscUtils.createFormatter(DataFieldMetadata.DATE_FIELD, 
    				 locale, pattern);
    		 this.locale = locale;
    		 position = new ParsePosition(0);
    	 }
    	 
    	 public void reset(String newLocale, String newPattern) {
			if (!newLocale.equals(locale)) {
				formatter = (SimpleDateFormat) MiscUtils.createFormatter(
						DataFieldMetadata.DATE_FIELD, newLocale, newPattern);
				this.locale = newLocale;
			}
			resetPattern(newPattern);
		}
    	 
    	 public void resetPattern(String newPattern) {
			if (!newPattern.equals(formatter.toPattern())) {
				formatter.applyPattern(newPattern);
			}
			position.setIndex(0);
		}
    	 
    	 public void setLenient(boolean lenient){
    		 formatter.setLenient(lenient);
    	 }
        }
        

        public Str2DateFunction() {
            super("convert", "str2date", new TLValueType[] { TLValueType.STRING, TLValueType.STRING, TLValueType.OBJECT, TLValueType.STRING }, 
                    TLValueType.DATE, 4, 2);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	Str2DateContext c = (Str2DateContext)context.getContext();
            TLValue val = c.value;

            if (params[0]==TLValue.NULL_VAL || params[1]==TLValue.NULL_VAL) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.STR2DATE.name()+" - NULL value not allowed");
            }
            if (!(params[0].type == TLValueType.STRING && params[1].type == TLValueType.STRING)){
                throw new TransformLangExecutorRuntimeException(params,
                		Function.STR2DATE.name()+" - wrong type of literal");
            }
            if (params.length == 3 && !(params[2].type == TLValueType.STRING || params[2].type == TLValueType.BOOLEAN)){
                throw new TransformLangExecutorRuntimeException(params,
                		Function.STR2DATE.name()+" - wrong type of literal");
           }
            if (params.length == 4 && !(params[3].type == TLValueType.BOOLEAN)){
                throw new TransformLangExecutorRuntimeException(params,
                		Function.STR2DATE.name()+" - wrong type of literal");
            }
            if (params[0].toString().length() == 0){
            	return TLValue.NULL_VAL;
            }else{
                String pattern = params[1].toString();
                String locale = params.length > 2 && params[2].type == TLValueType.STRING ? 
                		params[2].toString() : null;
                Boolean lenient = null;
                if (params.length == 3 && params[2].type == TLValueType.BOOLEAN){
                	lenient = ((TLBooleanValue)params[2]).getBoolean();
                }else if (params.length == 4) {
                	lenient = ((TLBooleanValue)params[2]).getBoolean();
                }
                if (c.formatter == null){
                	c.init(locale, pattern);
                }else if (locale != null) {
                	c.reset(locale, pattern);
                }else{
                	c.resetPattern(pattern);
                }
                if (lenient != null){
                	c.setLenient(lenient);
                }else{
                	c.setLenient(true);
                }
                Date result = c.formatter.parse(params[0].toString(), c.position);
                if (c.position.getIndex() != 0) {
                	((TLDateValue) val).getDate().setTime(result.getTime());
                }else{
                    throw new TransformLangExecutorRuntimeException(params,
                            Function.STR2DATE.name()+" - can't convert \"" + params[0] + "\" using format "+c.formatter.toPattern());
                }
            }
            
            return val;
        }

        @Override
        public TLContext createContext() {
            Str2DateContext con=new Str2DateContext();
            con.value=TLValue.create(TLValueType.DATE);

            TLContext<Str2DateContext> context=new TLContext<Str2DateContext>();
            context.setContext(con);
            
            return context;
        }
    }
    
    
    // 	DATE2NUM
    class Date2NumFunction extends TLFunctionPrototype {

        public Date2NumFunction() {
            super("convert", "date2num", new TLValueType[] { TLValueType.DATE, TLValueType.SYM_CONST }, 
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
            super("convert", "str2num", new TLValueType[] { TLValueType.STRING, TLValueType.SYM_CONST, TLValueType.INTEGER }, 
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

    // TRY_CONVERT
    class TryConvertFunction extends TLFunctionPrototype {

        public TryConvertFunction() {
            super("convert", "try_convert", new TLValueType[] { TLValueType.OBJECT, TLValueType.OBJECT, TLValueType.STRING }, 
                    TLValueType.OBJECT,3,2);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	TLValueType fromtype = params[0].type;
        	TLValueType toType = params[1].type;
        	
        	boolean canConvert = false;
        	if (fromtype == toType) {
        		if (fromtype == TLValueType.DECIMAL) {//check precision
					TLValue candidate = TLValue.create(toType);
					candidate.setValue(params[0].getNumeric());
					canConvert = candidate.compareTo(params[0]) == 0;
					if (!canConvert) {
						return TLValue.FALSE_VAL;
					}
        		}
        		params[1].setValue(params[0].getValue());
        		return TLValue.TRUE_VAL;
        	}
        	if (!(fromtype.isCompatible(toType) || toType.isCompatible(fromtype))) {
        		return TLValue.FALSE_VAL;
        	}
        	switch (fromtype) {
			case BOOLEAN:
        		canConvert = toType.isNumeric() ||	toType == TLValueType.STRING;
        		if (canConvert) {
					if (toType.isNumeric()) {//boolean --> numeric
						params[1].setValue(
								((TLBooleanValue)params[0]).getBoolean() ? 
										TLNumericValue.NUM_ONE_VAL : 
										TLNumericValue.NUM_ZERO_VAL);
					}else{//boolean --> string
						params[1].setValue(params[0].toString());
					}
				}
				break;
			case DATE:
        		canConvert = toType == TLValueType.STRING || toType == TLValueType.LONG;
        		//date --> string
        		if (canConvert && toType == TLValueType.STRING) {
        			DateFormat format;
        			if (params.length == 3) {
        				format = new SimpleDateFormat(params[2].toString());
        			}else{
        				format = DateFormat.getInstance();
        			}
        			params[1].setValue(format.format(((TLDateValue)params[0]).getDate()));
        		}
        		//date --> long
        		if (canConvert && toType == TLValueType.LONG){
        			((TLNumericValue<Numeric>)params[1]).setLong(((TLDateValue)params[0]).getDate().getTime());
        		}
				break;
			case LONG:
				if (toType == TLValueType.DATE) {
					canConvert = true;
					((TLDateValue)params[1]).setValue(new Date(params[0].getNumeric().getLong()));
				}
			case DECIMAL:
			case DOUBLE:
			case INTEGER:
				switch (toType) {
				case BOOLEAN://numeric --> boolean
					TLBooleanValue result = convertToBooleanValue((TLNumericValue<Numeric>)params[0]);
					if (result != null) {
						canConvert = true;
						params[1].setValue(result);
					}
					break;
				case STRING://numeric --> string
					canConvert = true;
					NumberFormat format;
        			if (params.length == 3) {
        				format = new NumericFormat(params[2].toString());
        			}else{
        				format = new NumericFormat();
        			}
        			params[1].setValue(format.format(params[0].getNumeric()));
        			break;
				case DECIMAL:
				case DOUBLE:
				case INTEGER:
				case LONG:
					//numeric --> another numeric 
					TLValue candidate = TLValue.create(toType);
					candidate.setValue(params[0].getNumeric());
					//TODO two comparison needed while can happen: cloverInt.compareTo(cloverDouble)=0 but cloverDouble.compareTo(cloverInt)=1 
					canConvert = candidate.compareTo(params[0]) == 0 && params[0].compareTo(candidate) == 0;
					if (canConvert) {
	        			params[1].setValue(candidate);
					}
				}
    			break;
			case STRING:
				Format format;
    			Number result = null;
				switch (toType) {
				case BOOLEAN://string --> boolean
					boolean isTrue = trueFormat.matches(params[0].toString());
					boolean isFalse = falseFormat.matches(params[0].toString());
					canConvert = isTrue || isFalse;
					if (canConvert) {
						params[1].setValue(isTrue ? TLValue.TRUE_VAL : TLValue.FALSE_VAL);
					}
					break;
				case BYTE://string --> byte
					canConvert = true;
					params[1].setValue(params[0].toString().getBytes());
					break;
				case DATE://string --> date
        			if (params.length == 3) {
        				format = new SimpleDateFormat(params[2].toString());
        			}else{
        				format = DateFormat.getInstance();
        			}
        			ParsePosition pos = new ParsePosition(0);
        			Date dateResult = ((SimpleDateFormat)format).parse(params[0].toString(), pos);
        			if (pos.getIndex() != 0) {
        				canConvert = true;
        				((TLDateValue)params[1]).setValue(dateResult);
        			}
        			break;
				case DECIMAL://string --> decimal
        			if (params.length == 3) {
        				format = new NumericFormat(params[2].toString());
        			}else{
        				format = new NumericFormat();
        			}
        			try {
        				result = (BigDecimal)((NumericFormat)format).parse(params[0].toString());
        				canConvert = ((BigDecimal)result).precision() <= params[1].getNumeric().getDecimal().getPrecision() &&
        							 ((BigDecimal)result).scale() <= params[1].getNumeric().getDecimal().getScale();
        				if (canConvert) {
        					params[1].setValue(result);
        				}
        			}catch (Exception e) {
						canConvert = false;
					}
        			break;
				case DOUBLE://string --> double
					if (params.length == 3) {
						format = new DecimalFormat(params[2].toString());
	        			try {
	        				result = ((DecimalFormat)format).parse(params[0].toString());
	        				canConvert = true;
	        			}catch (Exception e) {
							canConvert = false;
						}
					}else if (StringUtils.isNumber(params[1].toString())) {
						try {
							result = Double.parseDouble(params[1].toString());
	        				canConvert = true;
						} catch (Exception e) {
							canConvert = false;
						}
					}		
					if (canConvert) {
						params[1].setValue(result);
					}
					break;
				case LONG:
				case INTEGER:
					//string --> long
					int tmp = StringUtils.isInteger(params[0].toString());
					if (-1 < tmp && tmp < 3) {
						try {
							result = Long.parseLong(params[0].toString());
							canConvert = true;
						} catch (Exception e) {
							canConvert = false;
						}
					}					
					//if toType is integer, check if received number is in Integer range
					if (canConvert && toType == TLValueType.INTEGER && 
							(tmp < Integer.MIN_VALUE || Integer.MAX_VALUE < tmp)){
						canConvert = false;
					}
					if (canConvert) {
						params[1].setValue(result);
					}
					break;
				}
				break;
			case MAP:
			case LIST:
			case BYTE:
        		canConvert = toType == TLValueType.STRING;
        		if (canConvert) {
        			params[1].setValue(params[0].toString());
        		}
        		break;
        	}
        	
    		return canConvert ? TLValue.TRUE_VAL : TLValue.FALSE_VAL;
        }
    
    }

    private static TLBooleanValue convertToBooleanValue(TLNumericValue<Numeric> value){
    	if (value.compareTo(TLNumericValue.NUM_ONE_VAL) == 0) {
    		return TLBooleanValue.FALSE;
    	}
    	if (value.compareTo(TLNumericValue.NUM_ZERO_VAL) == 0){
    		return TLBooleanValue.TRUE;
    	}
    	return null;
    }
}
