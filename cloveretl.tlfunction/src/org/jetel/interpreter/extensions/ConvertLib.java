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

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.ByteArray;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.data.primitive.NumericFormat;
import org.jetel.data.primitive.StringFormat;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLByteArrayValue;
import org.jetel.interpreter.data.TLDateValue;
import org.jetel.interpreter.data.TLNullValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLRecordValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.interpreter.extensions.DateLib.CalendarStore;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.PackedDecimal;
import org.jetel.util.crypto.Digest;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.formatter.DateFormatterFactory;
import org.jetel.util.string.CloverString;


public class ConvertLib extends TLFunctionLibrary {

	private static final String LIBRARY_NAME = "Convert";

	public static final int DEFAULT_RADIX = 10;

	private static StringFormat trueFormat = StringFormat.create(Defaults.DEFAULT_REGEXP_TRUE_STRING);
	private static StringFormat falseFormat = StringFormat.create(Defaults.DEFAULT_REGEXP_FALSE_STRING);

	enum Function {
        NUM2STR("num2str"),
        DATE2STR("date2str"),
        STR2DATE("str2date"),
        DATE2NUM("date2num"),
        STR2NUM("str2num"),
        TRY_CONVERT("try_convert"),
        NUM2NUM("num2num"),
        NUM2BOOL("num2bool"),
        BOOL2NUM("bool2num"),
        STR2BOOL("str2bool"),
        LONG2DATE("long2date"),
        DATE2LONG("date2long"),
        TOSTRING("to_string"),
        BASE64BYTE("base64byte"),
        BYTE2BASE64("byte2base64"),
        BITS2STR("bits2str"),
        STR2BITS("str2bits"),
        HEX2BYTE("hex2byte"),
        BYTE2HEX("byte2hex"),
        LONG2PACKEDDECIMAL("long2pacdecimal"),
        PACKEDDECIMAL2LONG("pacdecimal2long"),
        MD5("md5"),
        SHA("sha"),
        GET_FIELD_NAME("get_field_name"),
        GET_FIELD_TYPE("get_field_type");
        
        
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

    @Override
	public TLFunctionPrototype getFunction(String functionName) {
        switch(Function.fromString(functionName)) {
        case NUM2STR: return new Num2StrFunction();
        case DATE2STR: return new Date2StrFunction();
        case STR2DATE: return new Str2DateFunction();
        case DATE2NUM: return new Date2NumFunction();
        case STR2NUM: return new Str2NumFunction();
        case TRY_CONVERT: return new TryConvertFunction();
        case BOOL2NUM: return new Bool2NumFunction();
        case DATE2LONG: return new Date2LongFunction();
        case LONG2DATE: return new Long2DateFunction();
        case NUM2BOOL: return new Num2BoolFunction();
        case NUM2NUM: return new Num2NumFunction();
        case STR2BOOL: return new Str2BoolFunction();
        case TOSTRING: return new ToStringFunction();
        case BYTE2BASE64: return new Byte2Base64Function();
        case BASE64BYTE: return new Base64ByteFunction();
        case BITS2STR: return new Bits2StrFunction();
        case STR2BITS: return new Str2BitsFunction();
        case HEX2BYTE: return new Hex2ByteFunction();
        case BYTE2HEX: return new Byte2HexFunction();
        case LONG2PACKEDDECIMAL: return new Long2PackedDecimalFunction();
        case PACKEDDECIMAL2LONG: return new PackedDecimal2LongFunction();
        case MD5: return new MD5Function();
        case SHA: return new SHAFunction();
        case GET_FIELD_NAME: return new GetFieldNameFunction();
        case GET_FIELD_TYPE: return new GetFieldTypeFunction();
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

    //  NUM2STR
    class Num2StrFunction extends TLFunctionPrototype {

        public Num2StrFunction() {
            super("convert", "num2str", "Returns string representation of a number in a given numeral system", 
            		new TLValueType[] { TLValueType.DECIMAL, TLValueType.STRING, TLValueType.STRING}, 
                    TLValueType.STRING, 3,1);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = ((Num2StrContext)context.getContext()).value;
            TLValue input=params[0];
            CloverString strBuf = (CloverString) val.getValue();
            strBuf.setLength(0);
            
            if (params[0]==TLNullValue.getInstance()) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.NUM2STR.name()+" - NULL value not allowed");
            }
            Integer radix = null;
            String formatString = null;
            
            if (params.length>1) {
                if (params[1]==TLNullValue.getInstance()){
                    throw new TransformLangExecutorRuntimeException(params,
                        Function.NUM2STR.name()+" - wrong type of literals");
                }
                if (params[1].getType().isNumeric()) {
					radix = ((TLNumericValue) params[1]).getInt();
				}else if (params[1].getType() == TLValueType.STRING) {
					formatString = params[1].toString();
				}else {
                    throw new TransformLangExecutorRuntimeException(params,
                            Function.NUM2STR.name()+" - wrong type of literals");
				}
            }
            
            if (radix == null && formatString == null) {
            	radix = 10;
            }
            
            if (radix != null) {
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
                    case NUMBER:
                        strBuf.append(Double.toHexString(((TLNumericValue)input).getDouble()));
                        break;
                    default:
                    throw new TransformLangExecutorRuntimeException(params,
                            Function.NUM2STR.name()+" - can't convert number to string using specified radix");
                    }
                }
             } else {//radix == null --> formatString != null
            	 
            	 
            	 String locale = null;
                 if (params.length>2) {
                     if (params[2].getType() !=TLValueType.STRING){
                         throw new TransformLangExecutorRuntimeException(params,
                             Function.NUM2STR.name()+" - wrong type of literals");
                     }
                     locale = params[2].toString();
                 } else {
                	 // locale not set - use system default
                	 locale = Defaults.DEFAULT_LOCALE;
                 }
            	 Num2StrContext c = (Num2StrContext)context.getContext();
            	 if (c.format == null || !c.locale.equals(locale) || !c.format.toPattern().equals(formatString)) {
            		 // reinit formatter on first entry, locale or pattern change
            		 c.init(locale, formatString);
            	 } 
            	 strBuf.append(c.format.format(((TLNumericValue)input).getDouble()));
            }

            
            return val;
        }

        @Override
        public TLContext createContext() {
            return Num2StrContext.createContex();
        }
    }

    
    
    //  DATE2STR
    class Date2StrFunction extends TLFunctionPrototype {
        
        public Date2StrFunction() {
            super("convert", "date2str", "Converts date to string based on a pattern",
            		new TLValueType[] { TLValueType.DATE, TLValueType.STRING }, 
                    TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = ((Date2StrContext)context.getContext()).value;
            SimpleDateFormat format=((Date2StrContext)context.getContext()).format;
            CloverString strBuf = (CloverString) val.getValue();
            strBuf.setLength(0);
            
            if (params[0]==TLNullValue.getInstance() || params[1]==TLNullValue.getInstance()) {
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
        	return Date2StrContext.createContex();
        }
    }

    //  STR2DATE
    class Str2DateFunction extends TLFunctionPrototype {
        

		public Str2DateFunction() {
			super("convert", "str2date",
					"Converts string to date based on a pattern",
					new TLValueType[] { TLValueType.STRING, TLValueType.STRING,
							TLValueType.STRING, TLValueType.BOOLEAN },
					TLValueType.DATE, 4, 2);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			Str2DateContext c = (Str2DateContext) context.getContext();
			TLDateValue val = c.value;
			String locale = null;
			String pattern = null;
			/*
			 * TODO: This code was temporarily fixed to use Joda datetime parser. Will have to be finalized in
			 * future releases to improve spped and polish the code
			 * 
			 */

			if (!c.parameterCache.isInitialized()) {
				if ((params[0].type != TLValueType.STRING)
						|| (params[1].type != TLValueType.STRING)) {
					throw new TransformLangExecutorRuntimeException(params,
							Function.STR2DATE.name()
									+ " - wrong type of literal");
				}
				if (params.length >= 3) {
					if (params[2].type == TLValueType.STRING) {
						locale = params[2].toString();
					} else {
						throw new TransformLangExecutorRuntimeException(params,
								Function.STR2DATE.name()
										+ " - wrong type of literal");
					}
					c.parameterCache.cache(params[1],params[2]);
				}else{
					c.parameterCache.cache(params[1]);
				}
				c.init(locale, 	pattern=params[1].toString());
			}else{
				if (params.length>=3){
					if (c.parameterCache.hasChanged(params[1],params[2])){
						c.reset(params[2].toString(), params[1].toString());
						c.parameterCache.cache(params[1],params[2]);
					}
				}else{
					if (c.parameterCache.hasChanged(params[1])){
							c.resetPattern(params[1].toString());
							c.parameterCache.cache(params[1]);
					}
				}
			
			}
			//Date result;
			//result = c.formatter.parse(params[0].toString(), c.position);
			if (params[0]==TLNullValue.getInstance()){
				throw new TransformLangExecutorRuntimeException(params,
					Function.STR2DATE.name()
							+ " - wrong type of literal");
			}
			if (params.length >= 4 && params[3].getType() == TLValueType.BOOLEAN) {
				c.setLenient(((TLBooleanValue) params[3]).getBoolean());
			}
			try{
				val.getDate().setTime( c.formatter.parseMillis(params[0].toString()) );
			}catch(Exception ex){
				throw new TransformLangExecutorRuntimeException(params,
					Function.STR2DATE.name() + " - can't convert \""
							+ params[0] + "\" using format "
							+ c.formatter.getPattern(), ex);
			}
		/*	if (result != null) {
				val.setValue(result);
			} else {
				throw new TransformLangExecutorRuntimeException(params,
						Function.STR2DATE.name() + " - can't convert \""
								+ params[0] + "\" using format "
								+ c.formatter.toPattern());
			}*/

			return val;
		}

        @Override
        public TLContext createContext() {
        	return Str2DateContext.createContext();
        }
    }
    
    
    // 	DATE2NUM
    class Date2NumFunction extends TLFunctionPrototype {

        public Date2NumFunction() {
            super("convert", "date2num", "Returns a number value of a date component (i.e. month)",
            		new TLValueType[] { TLValueType.DATE, TLValueType.SYM_CONST }, 
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
            super("convert", "str2num", "Converts string to number (from any numeral system)", 
            		new TLValueType[] { TLValueType.STRING, TLValueType.SYM_CONST, TLValueType.OBJECT, TLValueType.STRING }, 
                    TLValueType.INTEGER,4,1);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	if ((params[0].type!=TLValueType.STRING) || 
        			(params.length>1 && params[1].type!=TLValueType.SYM_CONST) || 
        			(params.length>2 && !(params[2].type.isNumeric() || params[2].type == TLValueType.STRING)) ||
        			(params.length>3 && params[3].type != TLValueType.STRING)){
                throw new TransformLangExecutorRuntimeException(params,
                        "str2num - wrong type of literals");
        	}
        	
        	// 2nd parameter - type/format
        	TLValueType valType = null;
        	String numFormat = null;
        	if (params.length > 1) {
           		valType = TLFunctionUtils.astToken2ValueType(params[1]);
        	} else {
        		valType = TLValueType.INTEGER;
        	}
        	
        	// 3nd parameter - radix/format
        	Str2NumContext con = (Str2NumContext)context.getContext();
        	int radix=DEFAULT_RADIX;
        	NumberFormat format = null;
        	if (params.length>2){
        		if (params[2].type.isNumeric()) {
					radix = ((TLNumericValue) params[2]).getInt();
					con.reset(numFormat, null, valType);
				}else {
					//format and locale
					con.reset(params[2].toString(), params.length>3 ? params[3].toString() : null, valType);
					format = con.format;
				}
        	}else{
        		con.reset(null, null, valType);
        	}
        	TLValue value=(TLValue)con.value;
        	
        	Number result = null;
        	try{
               	if (format != null) {
            		result = valType == TLValueType.DECIMAL ?
            				((NumericFormat)format).parse((CharSequence)params[0].toString()) :
            				format.parse(params[0].toString());
            	}else {
	                switch (valType) {
	                case INTEGER:
	                 	result = Integer.parseInt(params[0].toString(), radix);
	                    break;
	                case LONG:
	                	result = Long.parseLong(params[0].toString(), radix);
	                    break;
	                case DECIMAL:
	                    if (radix == 10) {
	                    	result = new BigDecimal(params[0].toString());
	                    } else {
	                        throw new TransformLangExecutorRuntimeException(params,
	                                "str2num - can't convert string to decimal number using specified radix");
	                    }
	                    break;
	                case NUMBER:
	                	if (radix==10 || radix==16){
	                		result = Double.parseDouble(params[0].toString());
	                }else{
	                        throw new TransformLangExecutorRuntimeException(params,
	                                "str2num - can't convert string to number/double number using specified radix");
	                    }
	                }
	            }
            } catch (Exception ex) {//NumberFormatException or ParseException
                throw new TransformLangExecutorRuntimeException(params, "str2num - can't convert \"" + params[0] + "\" " + 
                		(format != null ? "with format \"" + format +  "\"": "using radix " + radix));
            }
            if (value != null) {
            	value.setValue(result);
            }else{
            	value = new TLNumericValue<Decimal>(valType, DecimalFactory.getDecimal(result.toString()));
            }
        return value;
    }
    
	@Override
	public TLContext createContext() {
		return Str2NumContext.createContext();
	}
    }

    // 	NUM2NUM
    class Num2NumFunction extends TLFunctionPrototype {

        public Num2NumFunction() {
            super("convert", "num2num", "Converts numbers of different types",
            		new TLValueType[] { TLValueType.DECIMAL, TLValueType.SYM_CONST }, 
                    TLValueType.INTEGER, 2, 1);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	if (!params[0].type.isNumeric() || 
        			(params.length>1 && params[1].type!=TLValueType.SYM_CONST)){
                throw new TransformLangExecutorRuntimeException(params,
                        "num2num - wrong type of literals");
        	}
        	TLValueType valType = (params.length>1 ? TLFunctionUtils.astToken2ValueType(params[1]) : TLValueType.INTEGER);
        	TLValue value=(TLValue)context.getContext();
        	if (value==null  && !(valType == TLValueType.DECIMAL)){
        		// initialize
        		value=TLValue.create(valType);
        		context.setContext(value);
        	}
        	
        	if (valType != TLValueType.DECIMAL) {
				value.setValue(params[0]);
			}else{
				value = new TLNumericValue<Numeric>(TLValueType.DECIMAL, params[0].getNumeric().getDecimal());
			}
			if (value.compareTo(params[0]) != 0 || params[0].compareTo(value) != 0) {
                throw new TransformLangExecutorRuntimeException(params,
                        "num2num - can't convert \"" + params[0] + "\" to " + valType.getName());
        	}
        	
        	return value;
        }


        @Override
		public TLContext createContext() {
   		 TLContext<TLNumericValue<Numeric>> context = new TLContext<TLNumericValue<Numeric>>();
	        context.setContext(null);
	        return context;
		}
    }
    
    // 	NUM2BOOL
    class Num2BoolFunction extends TLFunctionPrototype {

        public Num2BoolFunction() {
            super("convert", "num2bool", "Converts 1 to true and 0 to false",  new TLValueType[] { TLValueType.DECIMAL}, 
                    TLValueType.BOOLEAN);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	if (!params[0].type.isNumeric()){
                throw new TransformLangExecutorRuntimeException(params,
                        "num2bool - wrong type of literals");
        	}
        	if (params[0].compareTo(TLNumericValue.ONE) == 0) {
        		return TLBooleanValue.TRUE;
        	}
        	if (params[0].compareTo(TLNumericValue.ZERO) == 0){
        		return TLBooleanValue.FALSE;
        	}
            throw new TransformLangExecutorRuntimeException(params,
                    "num2bool - can't convert \"" + params[0] + "\" to " + TLValueType.BOOLEAN.getName());       	
        }
    }

    //BOOL2NUM
    class Bool2NumFunction extends TLFunctionPrototype {

		public Bool2NumFunction() {
			super("convert", "bool2num", "Converts true to 1 and false to 0",
					new TLValueType[] { TLValueType.BOOLEAN, TLValueType.SYM_CONST}, 
					TLValueType.INTEGER, 2, 1);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
        	if (params[0].type!=TLValueType.BOOLEAN || 
        			(params.length == 2 && params[1].type!=TLValueType.SYM_CONST)){
                throw new TransformLangExecutorRuntimeException(params,
                        "bool2num - wrong type of literals");
        	}
        	TLValueType valType = (params.length>1 ? TLFunctionUtils.astToken2ValueType(params[1]) : TLValueType.INTEGER);
        	TLValue value=(TLValue)context.getContext();
        	if (value==null){
        		// initialize
        		value=TLValue.create(valType);
        		context.setContext(value);
        	}
        	value.setValue(((TLBooleanValue)params[0]).getBoolean() ? TLNumericValue.ONE : TLNumericValue.ZERO);
        	
        	return value;
		}

		@Override
		public TLContext createContext() {
			 TLContext<TLValue> context = new TLContext<TLValue>();
		        context.setContext(null);
		        return context;
		}
	}

    //STR2BOOL
	public class Str2BoolFunction extends TLFunctionPrototype {

		public Str2BoolFunction() {
			super("convert", "str2bool",  "Converts string to a boolean based on a pattern (i.e. \"true\")", 
					new TLValueType[] { TLValueType.STRING}, TLValueType.BOOLEAN);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			
			if (trueFormat.matches(params[0].toString())) return TLBooleanValue.TRUE;
			
			if (falseFormat.matches(params[0].toString())) return TLBooleanValue.FALSE;
			
            throw new TransformLangExecutorRuntimeException(params,
                   "str2bool - can't convert \"" + params[0] + "\" to " + TLValueType.BOOLEAN.getName());
        }
			
	}
	
    //toString
	public class ToStringFunction extends TLFunctionPrototype {

		public ToStringFunction() {
			super("convert", "to_string",  "Returns string representation of its argument",
					new TLValueType[] { TLValueType.OBJECT}, TLValueType.STRING);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLStringValue val = (TLStringValue)context.context;
			val.setValue(params[0].toString());
			return val;
        }
			
        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
	}

    //Long2Date
	public class Long2DateFunction extends TLFunctionPrototype {

		public Long2DateFunction() {
			super("convert", "long2date", "Returns date from long that represents milliseconds from epoch", 
					new TLValueType[] { TLValueType.LONG} , TLValueType.DATE);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
        	if (params[0].type!=TLValueType.LONG ){
                throw new TransformLangExecutorRuntimeException(params,
                        "long2date - wrong type of literals");
        	}
			TLValue val = (TLValue)context.getContext();
			if (!(val instanceof TLDateValue)) {
				val = TLValue.create(TLValueType.DATE);
				context.setContext(val);
			}
			val.setValue(new Date(((TLNumericValue<CloverLong>)params[0]).getLong()));
			return val;
		}

        @Override
        public TLContext createContext() {
            return TLContext.createDateContext();
        }
	}

    //DATE2LONG
	public class Date2LongFunction extends TLFunctionPrototype {

		public Date2LongFunction() {
			super("convert", "date2long", "Returns long that represents milliseconds from epoch to a date",
					new TLValueType[] { TLValueType.DATE} , TLValueType.LONG);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
        	if (params[0].type!=TLValueType.DATE ){
                throw new TransformLangExecutorRuntimeException(params,
                        "date2long - wrong type of literals");
        	}
			TLNumericValue value = (TLNumericValue)context.getContext();
        	if (value==null || !(value.type == TLValueType.LONG)){
        		// initialize
        		value=(TLNumericValue)TLValue.create(TLValueType.LONG);
        		context.setContext(value);
        	}
			value.setValue(((TLDateValue)params[0]).getDate().getTime());
			return value;
		}

        @Override
        public TLContext createContext() {
            return TLContext.createLongContext();
        }
	}

	//BASE64BYTE
	public class Base64ByteFunction extends TLFunctionPrototype {

		public Base64ByteFunction() {
			super("convert", "base64byte", "Converts binary data encoded in base64 to array of bytes",
					new TLValueType[] { TLValueType.STRING} , TLValueType.BYTE);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLByteArrayValue value = (TLByteArrayValue)context.getContext();
        	if (params[0].getType()!= TLValueType.STRING){
        		throw new TransformLangExecutorRuntimeException(params,
                        "base64byte - can't convert \"" + params[0] + "\" to " + TLValueType.BYTE.getName());
        	}
        	value.getByteAraray().decodeBase64(params[0].toString());
			return value;
		}

        @Override
        public TLContext createContext() {
            return TLContext.createByteContext();
        }
	}
	
	//	BYTE2BASE64
	public class Byte2Base64Function extends TLFunctionPrototype {

		public Byte2Base64Function() {
			super("convert", "byte2base64", "Converts binary data into their base64 representation",
					new TLValueType[] { TLValueType.BYTE} , TLValueType.STRING);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLStringValue value = (TLStringValue)context.getContext();
        	if (params[0].getType()!= TLValueType.BYTE){
        		throw new TransformLangExecutorRuntimeException(params,
                        "byte2base64 - can't convert \"" + params[0] + "\" to " + TLValueType.STRING.getName());
        	}
        	value.setValue(((TLByteArrayValue)params[0]).getByteAraray().encodeBase64());
			return value;
		}

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
	}


	
	//	 BITS2STR
	public class Bits2StrFunction extends TLFunctionPrototype {

		public Bits2StrFunction() {
			super("convert", "bits2str", "Converts bits into their string representation",
					new TLValueType[] { TLValueType.BYTE} , TLValueType.STRING);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLStringValue value = (TLStringValue)context.getContext();
        	if (params[0].getType()!= TLValueType.BYTE){
        		throw new TransformLangExecutorRuntimeException(params,
                        "bits2str - can't convert \"" + params[0] + "\" to " + TLValueType.STRING.getName());
        	}
        	ByteArray bits=((TLByteArrayValue)params[0]).getByteAraray();
        	int length = bits.length();
        	value.setValue(bits.decodeBitString('1', '0', 0, length == 0 ? 0 : (bits.length()<<3)-1));
			return value;
		}

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
	}

	//	 STR2BITS
	public class Str2BitsFunction extends TLFunctionPrototype {

		public Str2BitsFunction() {
			super("convert", "str2bits", "Converts string representation of bits into binary value",
					new TLValueType[] { TLValueType.STRING} , TLValueType.BYTE);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLByteArrayValue value = (TLByteArrayValue)context.getContext();
        	if (params[0].getType()!= TLValueType.STRING){
        		throw new TransformLangExecutorRuntimeException(params,
                        "str2bits - can't convert \"" + params[0] + "\" to " + TLValueType.BYTE.getName());
        	}
        	value.getByteAraray().encodeBitString((TLStringValue)params[0], '1', true);
			return value;
		}

        @Override
        public TLContext createContext() {
        	return TLContext.createByteContext();
        }
	}
	
	//	BYTE2HEX
	public class Byte2HexFunction extends TLFunctionPrototype {

		public Byte2HexFunction() {
			super("convert", "byte2hex", "Converts binary data into hex string",
					new TLValueType[] { TLValueType.BYTE} , TLValueType.STRING);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLStringValue value = (TLStringValue)context.getContext();
        	if (params[0].getType()!= TLValueType.BYTE){
        		throw new TransformLangExecutorRuntimeException(params,
                        "byte2hex - can't convert \"" + params[0] + "\" to " + TLValueType.STRING.getName());
        	}
        	CloverString strVal = (CloverString) value.getValue();
        	ByteArray bytes=((TLByteArrayValue)params[0]).getByteAraray();
        	strVal.setLength(0);
        	strVal.ensureCapacity(bytes.length());
    		for(int i=0;i<bytes.length(); i++){
    			strVal.append(Character.forDigit((bytes.getByte(i) &0xF0)>>4,16));
    			strVal.append(Character.forDigit(bytes.getByte(i)&0x0F,16));
    		}
        	return value;
		}

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
	}

	//	HEX2BYTE
	public class Hex2ByteFunction extends TLFunctionPrototype {

		public Hex2ByteFunction() {
			super("convert", "hex2byte", "Converts hex string into binary",
					new TLValueType[] { TLValueType.STRING} , TLValueType.BYTE);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLByteArrayValue value = (TLByteArrayValue)context.getContext();
        	if (params[0].getType()!= TLValueType.STRING){
        		throw new TransformLangExecutorRuntimeException(params,
                        "hex2byte - can't convert \"" + params[0] + "\" to " + TLValueType.BYTE.getName());
        	}
			ByteArray bytes=value.getByteAraray();
        	bytes.reset();
        	CharSequence chars=(TLStringValue)params[0];
    		for(int i=0; i<chars.length()-1;i=i+2){
    			bytes.append((byte)(((byte)Character.digit(chars.charAt(i),16)<<4)|(byte)Character.digit(chars.charAt(i+1),16)));
    		}
        	return value;
		}

        @Override
        public TLContext createContext() {
            return TLContext.createByteContext();
        }
	}
	
	
	
	
    // TRY_CONVERT
    class TryConvertFunction extends TLFunctionPrototype {
    	
    	private final static int FROM_INDEX = 0;
    	private final static int TO_INDEX = 1;
    	private final static int FORMAT_INDEX = 2;
		private static final int LOCALE_INDEX = 3;

        public TryConvertFunction() {
            super("convert", "try_convert", "Tries to convert variable of one type to another",  new TLValueType[] { TLValueType.OBJECT, TLValueType.OBJECT, TLValueType.STRING }, 
                    TLValueType.OBJECT,4,2);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	if (params.length < 2 || params[1].type != TLValueType.SYM_CONST) {
        		throw new TransformLangExecutorRuntimeException("try_convert: second parameter must be a type name");
        	}
        	
        	TLValueType fromType = params[0].type;
        	TLValueType toType = (params.length>1 ? TLFunctionUtils.astToken2ValueType(params[1]) : TLValueType.INTEGER);

        	TLValue ret = TLValue.create(toType);
        	
        	boolean canConvert = false;
        	if (fromType == toType) {
        		if (fromType == TLValueType.DECIMAL) {//check precision
					ret.setValue(params[0].getNumeric());
					canConvert = ret.compareTo(params[0]) == 0;
					if (!canConvert) {
						return TLNullValue.getInstance();
					}
        		}
        		ret.setValue(params[0].getValue());
        		return ret;
        	}
        	
        	TLFunctionPrototype convertFunction = getConvertToFunction(fromType, toType);
        	if (convertFunction == null) {
        		return TLNullValue.getInstance();
        	}
        	
        	try{
        		ret.setValue(convertFunction.execute(getConvertParams(convertFunction, params), 
        				getConvertContext(convertFunction, context)));
        	}catch (TransformLangExecutorRuntimeException e) {
				return TLNullValue.getInstance();
			}
        	
    		return ret;
        }
        
        @Override
        public TLContext createContext() {
			 TLContext<TLValue> context = new TLContext<TLValue>();
		        context.setContext(null);
		        return context;
        }
        
        private TLContext getConvertContext(TLFunctionPrototype function, TLContext context){
        	if (function instanceof Num2StrFunction && !(context.getContext() instanceof Num2StrContext)) {
        		return Num2StrContext.createContex();
        	}
        	if (function instanceof Date2StrFunction && !(context.getContext() instanceof Date2StrContext)){
        		return Date2StrContext.createContex();
        	}
        	if (function instanceof Str2DateFunction && !(context.getContext() instanceof Str2DateContext)){
        		return Str2DateContext.createContext();
        	}
           	if (function instanceof Num2NumFunction || function instanceof Bool2NumFunction || 
           			function instanceof Date2LongFunction &&	!(context.getContext() instanceof TLNumericValue)){
           		return TLContext.createNullContext();
           	}
           	if (function instanceof Long2DateFunction && !(context.getContext() instanceof TLDateValue)){
           		return TLContext.createDateContext();
           	}
           	if (function instanceof Str2NumFunction && !(context.getContext() instanceof Str2NumContext)){
           		return Str2NumContext.createContext();
           	}
           	return TLContext.createStringContext();
        }
        
        private TLValue[] getConvertParams(TLFunctionPrototype function, TLValue[] convertParams){
        	if (function instanceof Num2StrFunction){
        		TLValue[] result = null;
        		if (convertParams.length > 3) {
        			result = new TLValue[]{convertParams[0],convertParams[2],convertParams[3]};
        		} else if (convertParams.length > 2) {
        			result= new TLValue[]{convertParams[0],convertParams[2]};
        		} else {
        			result= new TLValue[]{convertParams[0]};
        		}
        		return result;
        	}
        	if (function instanceof Date2StrFunction || function instanceof Str2DateFunction) {
        		return new TLValue[]{convertParams[FROM_INDEX], convertParams[FORMAT_INDEX]};
        	}
        	if (function instanceof Num2NumFunction || function instanceof Bool2NumFunction) {
        		return new TLValue[]{convertParams[FROM_INDEX], convertParams[TO_INDEX]};
        	}
        	if (function instanceof Str2NumFunction ) {
        		TLValue[] result = new TLValue[convertParams.length];
        		System.arraycopy(convertParams, 0, result, 0, convertParams.length);
        		return result;
        	}
         	return new TLValue[]{convertParams[FROM_INDEX]};
        }
    
    }

    private TLFunctionPrototype getConvertToFunction(TLValueType fromType, TLValueType toType){
    	if (fromType != TLValueType.BOOLEAN && toType != TLValueType.BOOLEAN &&
    			!(fromType.isCompatible(toType) || toType.isCompatible(fromType))) {
    		return null;
    	}
    	switch (fromType) {
        case INTEGER:
        case LONG:
        case NUMBER:
        case DECIMAL:
        	switch (toType) {
            case INTEGER:
            case LONG:
            case NUMBER:
            case DECIMAL:
				return new Num2NumFunction();
            case BOOLEAN:
            	return new Num2BoolFunction();
            case DATE:
            	return fromType == TLValueType.LONG ? new Long2DateFunction() : null;
            case BYTE:
            case STRING:
            	return new Num2StrFunction();
			default:
				return null;
			}
        case BOOLEAN:
        	switch (toType) {
            case INTEGER:
            case LONG:
            case NUMBER:
            case DECIMAL:
            	return new Bool2NumFunction();
            case BYTE:
            case STRING:
            	return new ToStringFunction();
			default:
				return null;
			}
        case DATE:
        	switch (toType) {
			case LONG:
				return new Date2LongFunction();
			case STRING:
				return new Date2StrFunction();
			default:
				return null;
			}
        case STRING:
        	switch (toType) {
            case INTEGER:
            case LONG:
            case NUMBER:
            case DECIMAL:
            	return new Str2NumFunction();
            case BOOLEAN:
            	return new Str2BoolFunction();
            case DATE:
            	return new Str2DateFunction();
			default:
				return null;
			}
        default:
			return new ToStringFunction();
		}
    }

}

		// LONG2PACKEDDECIMAL
		class Long2PackedDecimalFunction extends TLFunctionPrototype {
		
			public Long2PackedDecimalFunction() {
				super("convert", "long2pacdecimal",
					"Converts long into packed decimal representation (bytes)",
					new TLValueType[] { TLValueType.LONG }, TLValueType.BYTE);
		}
		
		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLByteArrayValue value = (TLByteArrayValue) context.getContext();
			if (!params[0].getType().isNumeric()) {
				throw new TransformLangExecutorRuntimeException(params,
						"long2pacdecimal - can't convert \"" + params[0] + "\" to "
									+ TLValueType.BYTE.getName());
				}
				ByteArray bytes = value.getByteAraray();
				bytes.ensureCapacity(16);
				byte[] tmp=new byte[16];
				int length = PackedDecimal.format(params[0].getNumeric().getLong(),
						tmp);
				bytes.setValue(tmp);
				bytes.setLength(length);
		
				return value;
			}
		
			@Override
			public TLContext createContext() {
				return TLContext.createByteContext();
			}
		}
		
		// PACKEDDECIMAL2LONG
		class PackedDecimal2LongFunction extends TLFunctionPrototype {
		
			public PackedDecimal2LongFunction() {
				super("convert", "pacdecimal2long",
					"Converts packed decimal(bytes) into long value",
					new TLValueType[] { TLValueType.BYTE }, TLValueType.LONG);
		}
		
		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLNumericValue value = (TLNumericValue) context.getContext();
			if (params[0].getType() != TLValueType.BYTE) {
				throw new TransformLangExecutorRuntimeException(params,
						"pacdecimal2long - can't convert \"" + params[0] + "\" to "
									+ TLValueType.LONG.getName());
				}
				value.setLong(PackedDecimal.parse(((TLByteArrayValue) params[0])
						.getByteAraray().getValue()));
				return value;
			}
		
			@Override
			public TLContext createContext() {
				return TLContext.createLongContext();
			}
		}
		
		// MD5
		class MD5Function extends TLFunctionPrototype {
		
			public MD5Function() {
				super("convert", "md5", "Calculates MD5 hash of input bytes or string",
					new TLValueType[] { TLValueType.OBJECT }, TLValueType.BYTE);
		}
		
		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLByteArrayValue value = (TLByteArrayValue) context.getContext();
			byte[] resultMD5;
			if (params[0].getType() == TLValueType.STRING) {
				resultMD5 = Digest.digest(Digest.DigestType.MD5, params[0]
						.toString());
			} else if (params[0].getType() == TLValueType.BYTE) {
				resultMD5 = Digest.digest(Digest.DigestType.MD5,
						((TLByteArrayValue) params[0]).getByteAraray().getValue());
			} else {
				throw new TransformLangExecutorRuntimeException(params,
						"md5 - can't convert \"" + params[0] + "\" to "
									+ TLValueType.BYTE.getName());
				}
		
				ByteArray bytes = value.getByteAraray();
				bytes.reset();
				bytes.append(resultMD5);
				return value;
			}
		
			@Override
			public TLContext createContext() {
				return TLContext.createByteContext();
			}
		}
		
		// SHA
		class SHAFunction extends TLFunctionPrototype {
		
			public SHAFunction() {
				super("convert", "sha", "Calculates SHA hash of input bytes or string",
					new TLValueType[] { TLValueType.OBJECT }, TLValueType.BYTE);
		}
		
		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLByteArrayValue value = (TLByteArrayValue) context.getContext();
			byte[] resultSHA;
			if (params[0].getType() == TLValueType.STRING) {
				resultSHA = Digest.digest(Digest.DigestType.SHA, params[0]
						.toString());
			} else if (params[0].getType() == TLValueType.BYTE) {
				resultSHA = Digest.digest(Digest.DigestType.SHA,
						((TLByteArrayValue) params[0]).getByteAraray().getValue());
			} else {
				throw new TransformLangExecutorRuntimeException(params,
						"sha - can't convert \"" + params[0] + "\" to "
								+ TLValueType.BYTE.getName());
			}
		
			ByteArray bytes = value.getByteAraray();
			bytes.reset();
			bytes.append(resultSHA);
			return value;
		}
		
		@Override
		public TLContext createContext() {
			return TLContext.createByteContext();
		}
}

		//GET_FIELD_NAME
		class GetFieldNameFunction extends TLFunctionPrototype {

			public GetFieldNameFunction() {
				super("convert", "get_field_name", "Returns name of i-th field of passed-in record",
						new TLValueType[] { TLValueType.RECORD, TLValueType.INTEGER} , TLValueType.STRING);
			}

			@Override
			public TLValue execute(TLValue[] params, TLContext context) {
				TLStringValue value = (TLStringValue)context.getContext();
		   	if (params[0].getType()!= TLValueType.RECORD || !params[1].getType().isNumeric()){
		   		throw new TransformLangExecutorRuntimeException(params,
		                   "get_field_name - - wrong type of literals");
		   	}
		   	try{
		   		value.setValue(((DataRecord)((TLRecordValue)params[0]).getValue()).getField(params[1].getNumeric().getInt()).getMetadata().getName());
		   		return value;
		   	}catch(Exception ex){
		   		return TLStringValue.EMPTY;
		   	}
			}

		   @Override
		   public TLContext createContext() {
		   	return TLContext.createStringContext();
		   }
		}

		//GET_FIELD_TYPE
		class GetFieldTypeFunction extends TLFunctionPrototype {

			public GetFieldTypeFunction() {
				super("convert", "get_field_type", "Returns data type of i-th field of passed-in record",
						new TLValueType[] { TLValueType.RECORD, TLValueType.INTEGER} , TLValueType.STRING);
			}

			@Override
			public TLValue execute(TLValue[] params, TLContext context) {
				TLStringValue value = (TLStringValue)context.getContext();
		   	if (params[0].getType()!= TLValueType.RECORD || !params[1].getType().isNumeric()){
		   		throw new TransformLangExecutorRuntimeException(params,
		                   "get_field_type - - wrong type of literals");
		   	}
		   	try{
		   		value.setValue(((DataRecord)((TLRecordValue)params[0]).getValue()).getField(params[1].getNumeric().getInt()).getMetadata().getTypeAsString());
		   		return value;
		   	}catch(Exception ex){
		   		return TLStringValue.EMPTY;
		   	}
			}

		   @Override
		   public TLContext createContext() {
		   	return TLContext.createStringContext();
		   }
		}


class Date2StrContext {
    TLValue value;
    SimpleDateFormat format;
    
    static TLContext createContex(){
        Date2StrContext con=new Date2StrContext();
        con.value=TLValue.create(TLValueType.STRING);
        con.format=new SimpleDateFormat();

        TLContext<Date2StrContext> context=new TLContext<Date2StrContext>();
        context.setContext(con);
        
        return context;        	
    }
}

class Str2DateContext {
	TLDateValue value;
	TLParameterCache parameterCache;
	DateFormatter formatter;

	public void init(String localePar, String pattern) {
		formatter = DateFormatterFactory.getFormatter(pattern, localePar);
	}

	public void reset(String newLocale, String newPattern) {
		formatter = DateFormatterFactory.getFormatter(newPattern, newLocale);
	}

	public void resetPattern(String newPattern) {
		formatter = DateFormatterFactory.getFormatter(newPattern);
	}

	public void setLenient(boolean lenient) {
		formatter.setLenient(lenient);
	}

	static TLContext createContext(){
        Str2DateContext con=new Str2DateContext();
        con.value=(TLDateValue)TLValue.create(TLValueType.DATE);
        con.parameterCache=new TLParameterCache();
        
        TLContext<Str2DateContext> context=new TLContext<Str2DateContext>();
        context.setContext(con);
        
        return context;
	}
}

class Num2StrContext {
    public String locale;
	TLValue value;
    DecimalFormat format;
    
    public void init(String locale, String pattern) {
    	this.locale = locale;
    	format = (DecimalFormat)MiscUtils.createFormatter(DataFieldMetadata.NUMERIC_FIELD, locale, pattern);
    }
    
	static TLContext createContex(){
        Num2StrContext con=new Num2StrContext();
        con.value=TLValue.create(TLValueType.STRING);

        TLContext<Num2StrContext> context=new TLContext<Num2StrContext>();
        context.setContext(con);
        
        return context;        	
    }
}


class Str2NumContext{
	TLValue value;
	NumberFormat format;
	String oldLocale;
	
	public void init(String pattern, String locale, TLValueType type){
		if (pattern != null) {
			switch (type) {
			case DECIMAL:
				//NumericFormat is not good because of eg str2num("5,46 K?????","0.## ????","cs.CZ") //K?????
				format = locale == null ?
						new NumericFormat(pattern):
						new NumericFormat(pattern, new DecimalFormatSymbols(MiscUtils.createLocale(locale)));
				break;
			case NUMBER:
			case INTEGER:
			case LONG:
				format = locale == null ?
					new DecimalFormat(pattern):
					new DecimalFormat(pattern, new DecimalFormatSymbols(MiscUtils.createLocale(locale)));
				break;
			default:
				throw new IllegalArgumentException(
						"Str2NumContex can't be defined for " + type.getName());
			}
		}
		if (type == TLValueType.DECIMAL) {
			value = null;
		}else if (value == null || value.type != type) {
			value = TLValue.create(type);
		}
		
	}
	
	public String toPattern(){
		if (format != null) {
			if (format instanceof NumericFormat) {
				return ((NumericFormat)format).toPattern();
			}
			return ((DecimalFormat)format).toPattern();
		}
		return null;
	}
	
	public void reset(String newPattern, String newLocale, TLValueType newType){
		if (newType == TLValueType.DECIMAL || value == null || newType != value.type) {
			init(newPattern, newLocale, newType);
		}else if (newLocale != null && !newLocale.equals(oldLocale) || (oldLocale != null && newLocale == null)) { 
			init(newPattern, newLocale, newType);
			oldLocale = newLocale;
		}else if (newPattern != null && !newPattern.equals(toPattern())) {
			if (format == null) {
				init(newPattern, newLocale, newType);
			}else if (format instanceof NumericFormat) {
				if (newLocale == null) ((NumericFormat)format).applyPattern(newPattern);
				else ((NumericFormat)format).applyLocalizedPattern(newPattern);
			}else {
				if (newLocale == null) ((DecimalFormat)format).applyPattern(newPattern);
				else ((DecimalFormat)format).applyLocalizedPattern(newPattern);
			}
		}
	}

	static TLContext createContext(){
		Str2NumContext con=new Str2NumContext();
        con.init(null, null, TLValueType.DECIMAL);

        TLContext<Str2NumContext> context=new TLContext<Str2NumContext>();
        context.setContext(con);
        
        return context;
	}

	
	
}

