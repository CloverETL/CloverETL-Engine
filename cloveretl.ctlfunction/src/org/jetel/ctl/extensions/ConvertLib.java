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
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.data.DateFieldEnum;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.extensions.TLFunctionAnnotation;
import org.jetel.ctl.extensions.TLFunctionLibrary;
import org.jetel.ctl.extensions.TLFunctionPrototype;
import org.jetel.data.DataField;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.StringFormat;
import org.jetel.util.MiscUtils;

public class ConvertLib extends TLFunctionLibrary {

	private static final String LIBRARY_NAME = "Convert";

	public static final int DEFAULT_RADIX = 10;

	private static StringFormat trueFormat = StringFormat
			.create(Defaults.DEFAULT_REGEXP_TRUE_STRING);
	private static StringFormat falseFormat = StringFormat
			.create(Defaults.DEFAULT_REGEXP_FALSE_STRING);


	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		TLFunctionPrototype ret = 
			"num2str".equals(functionName) ? new Num2StrFunction() :
			"date2str".equals(functionName) ? new Date2StrFunction() :
			"str2date".equals(functionName) ? new Str2DateFunction() :
			"date2num".equals(functionName) ? new Date2NumFunction() : 
			"str2int".equals(functionName) ? new Str2IntFunction() :
			"str2long".equals(functionName) ? new Str2LongFunction() :
			"str2double".equals(functionName) ? new Str2DoubleFunction() :
			"str2decimal".equals(functionName) ? new Str2DecimalFunction() :
			null;
		
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
		
		return ret;
			
	}
	
	// NUM2STR
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(Integer num, int radix) {
		return Integer.toString(num, radix);
	}
	@TLFunctionAnnotation("Returns string representation in decimal radix")
	public static final String num2str(Integer num) {
		return num2str(num,10);
	}
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(Long num, int radix) {
		return Long.toString(num, radix);
	}
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(Long num) {
		return num2str(num,10);
	}
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(Double num, int radix) {
		switch (radix) {
		case 10:
			return Double.toString(num);
		case 16: 
			return Double.toHexString(num);
		default:
			throw new TransformLangExecutorRuntimeException("num2str only support radix 10 and 16");
		} 
	}
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(Double num) {
		return num2str(num,10);
	}
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(BigDecimal num) {
		return num.toString();
	}
	class Num2StrFunction implements TLFunctionPrototype {
		
		public void execute(Stack stack, TLType[] actualParams) {
			int radix = 10;
			if (actualParams.length == 2) {
				radix = stack.popInt();
			}
			
			if (actualParams[0].isInteger()) {
				stack.push(num2str(stack.popInt(),radix));
			} else if (actualParams[0].isLong()) {
				stack.push(num2str(stack.popLong(),radix));
			} else if (actualParams[0].isDouble()) {
				stack.push(num2str(stack.popDouble(),radix));
			} else if (actualParams[0].isDecimal()) {
				stack.push(num2str(stack.popDecimal()));
			}
		}
	}

	@TLFunctionAnnotation("Converts date to string according to the specified pattern.")
	public static final String date2str(Date date, String pattern) {
		final SimpleDateFormat format = new SimpleDateFormat();
		format.applyPattern(pattern);
		return format.format(date);
	}
	// DATE2STR
	class Date2StrFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final String pattern = stack.popString();
			final Date date = stack.popDate();
			stack.push(date2str(date,pattern));
		}
	}

	// STR2DATE
	@TLFunctionAnnotation("Converts string to date based on a pattern")
	public static final Date str2date(String input, String pattern, String locale, boolean lenient) {
		SimpleDateFormat format = new SimpleDateFormat(pattern,MiscUtils.createLocale(locale));
		format.setLenient(lenient);
		ParsePosition p = new ParsePosition(0);
		return format.parse(input, p);
	}
	
	@TLFunctionAnnotation("Converts string to date based on a pattern")
	public static final Date str2date(String input, String pattern, String locale) {
		return str2date(input,pattern,locale,false);
	}

	@TLFunctionAnnotation("Converts string to date based on a pattern")
	public static final Date str2date(String input, String pattern) {
		return str2date(input,pattern,"en.US",false);
	}

	class Str2DateFunction implements TLFunctionPrototype {
		public void execute(Stack stack, TLType[] actualParams) {
			boolean lenient = false;
			String locale = "en.US";
			
			if (actualParams.length > 2) {
				
				if (actualParams.length > 3 ) {
					lenient = stack.popBoolean();
				}
				
				locale = stack.popString();
			}
			
			final String pattern = stack.popString();
			final String input = stack.popString();
		
			stack.push(str2date(input,pattern,locale,lenient));
		}
	}

	// DATE2NUM
	@TLFunctionAnnotation("Returns numeric value of a date component (i.e. month)")
	public static final Integer date2num(Date input, DateFieldEnum field) {
		Calendar c = Calendar.getInstance();
		c.setTime(input);
		switch (field) {
		case YEAR:
			return c.get(Calendar.YEAR);
		case MONTH:
			return c.get(Calendar.MONTH);
		case WEEK:
			return c.get(Calendar.WEEK_OF_YEAR);
		case DAY:
			return c.get(Calendar.DAY_OF_MONTH);
		case HOUR:
			return c.get(Calendar.HOUR_OF_DAY);
		case MINUTE:
			return c.get(Calendar.MINUTE);
		case SECOND:
			return c.get(Calendar.SECOND);
		case MILLISEC:
			return c.get(Calendar.MILLISECOND);
		default:
			throw new TransformLangExecutorRuntimeException("Unknown date field: " + field.name());
		}
	}
	class Date2NumFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final DateFieldEnum field = (DateFieldEnum)stack.pop();
			final Date input = stack.popDate();
			stack.push(date2num(input,field));
		}
	}

	@TLFunctionAnnotation("Parses string to integer using specific numeral system.")
	public static final Integer str2int(String input, Integer radix) {
		return Integer.valueOf(input,radix);
	}
	
	@TLFunctionAnnotation("Parses string to integer using specific numeral system.")
	public static final Integer str2int(String input) {
		return Integer.valueOf(input,10);
	}
	class Str2IntFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			int radix = 10;
			if (actualParams.length == 2) {
				radix = stack.popInt();
			}
			final String input = stack.popString();
			stack.push(str2int(input,radix));
		}
	}

	@TLFunctionAnnotation("Parses string to long using specific numeral system.")
	public static final Long str2long(String input, Integer radix) {
		return Long.valueOf(input,radix);
	}
	@TLFunctionAnnotation("Parses string to long using specific numeral system.")
	public static final Long str2long(String input) {
		return Long.valueOf(input,10);
	}
	class Str2LongFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			int radix = 10;
			if (actualParams.length == 2) {
				radix = stack.popInt();
			}
			final String input = stack.popString();
			stack.push(str2long(input,radix));
		}
	}

	@TLFunctionAnnotation("Parses string to double using specific numeral system.")
	public static final Double str2double(String input) {
		return Double.valueOf(input);
	}
	class Str2DoubleFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final String input = stack.popString();
			stack.push(str2double(input));
		}
	}

	@TLFunctionAnnotation("Parses string to double using specific numeral system.")
	public static final BigDecimal str2decimal(String input) {
		return new BigDecimal(input,TransformLangExecutor.MAX_PRECISION);
	}
	class Str2DecimalFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final String input = stack.popString();
			stack.push(str2decimal(input));
		}
	}

	// NUM2NUM
//	class Num2NumFunction implements TLFunctionPrototype {
//
//		public Num2NumFunction() {
//			super("convert", "num2num", "Converts numbers of different types",
//					new TLType[][] {
//							{ TLTypePrimitive.INTEGER, TLTypePrimitive.LONG,
//									TLTypePrimitive.DOUBLE, TLTypePrimitive.DECIMAL },
//							{ TLType.createTypeSymbol(TransformLangParserConstants.INT_VAR) } }, TLType.OBJECT, 2, 1);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLValueType valType = (params.length > 1 ? TLFunctionUtils
//					.astToken2ValueType(params[1]) : TLValueType.INTEGER);
//			TLValue value = (TLValue) context.getContext();
//			if (value == null && !(valType == TLValueType.DECIMAL)) {
//				// initialize
//				value = TLValue.create(valType);
//				context.setContext(value);
//			}
//
//			if (valType != TLValueType.DECIMAL) {
//				value.setValue(params[0]);
//			} else {
//				value = new TLNumericValue<Numeric>(TLValueType.DECIMAL,
//						params[0].getNumeric());
//			}
//			if (value.compareTo(params[0]) != 0
//					|| params[0].compareTo(value) != 0) {
//				throw new TransformLangExecutorRuntimeException(params,
//						"num2num - can't convert \"" + params[0] + "\" to "
//								+ valType.getName());
//			}
//
//			return value;
//		}
//
//		@Override
//		public TLContext createContext() {
//			TLContext<TLNumericValue<Numeric>> context = new TLContext<TLNumericValue<Numeric>>();
//			context.setContext(null);
//			return context;
//		}
//
//		@Override
//		public TLType checkParameters(TLType[] parameters) {
//			if (parameters.length < minParams || parameters.length > maxParams) {
//				return TLType.ERROR;
//			}
//
//			if (!parameters[0].isNumeric()) {
//				return TLType.ERROR;
//			}
//
//			TLType retType = TLTypePrimitive.INTEGER;
//			if (parameters.length > 1) {
//				if (!parameters[1].isTypeSymbol()) {
//					return TLType.ERROR;
//				}
//
//				TLTypeSymbol typeSym = (TLTypeSymbol) parameters[1];
//				if (!typeSym.representsType()) {
//					return TLType.ERROR;
//				}
//
//				retType = typeSym.getRepresentedType();
//
//				if (!retType.isNumeric()) {
//					return TLType.ERROR;
//				}
//
//			}
//
//			return retType;
//		}
//	}
//
//	// NUM2BOOL
//	class Num2BoolFunction implements TLFunctionPrototype {
//
//		public Num2BoolFunction() {
//			super("convert", "num2bool", "Converts 1 to true and 0 to false",
//					new TLType[][] { { TLTypePrimitive.INTEGER,
//							TLTypePrimitive.LONG, 
//							TLTypePrimitive.DOUBLE, TLTypePrimitive.DECIMAL } },
//					TLTypePrimitive.BOOLEAN);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			if (params[0].compareTo(TLNumericValue.ONE) == 0) {
//				return TLBooleanValue.TRUE;
//			}
//			if (params[0].compareTo(TLNumericValue.ZERO) == 0) {
//				return TLBooleanValue.FALSE;
//			}
//			throw new TransformLangExecutorRuntimeException(params,
//					"num2bool - can't convert \"" + params[0] + "\" to "
//							+ TLValueType.BOOLEAN.getName());
//		}
//
//		@Override
//		public TLType checkParameters(TLType[] parameters) {
//			if (parameters.length < minParams || parameters.length > maxParams) {
//				return TLType.ERROR;
//			}
//
//			if (!parameters[0].isNumeric()) {
//				return TLType.ERROR;
//			}
//
//			return TLTypePrimitive.BOOLEAN;
//		}
//	}
//
//	// BOOL2NUM
//	class Bool2NumFunction implements TLFunctionPrototype {
//
//		public Bool2NumFunction() {
//			super("convert", "bool2num", "Converts true to 1 and false to 0",
//					new TLType[][] { { TLTypePrimitive.BOOLEAN },
//							{ TLType.createTypeSymbol(TransformLangParserConstants.INT_VAR) } }, TLType.OBJECT, 2, 1);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLValueType valType = (params.length > 1 ? TLFunctionUtils
//					.astToken2ValueType(params[1]) : TLValueType.INTEGER);
//			TLValue value = (TLValue) context.getContext();
//			if (value == null) {
//				// initialize
//				value = TLValue.create(valType);
//				context.setContext(value);
//			}
//			value
//					.setValue(((TLBooleanValue) params[0]).getBoolean() ? TLNumericValue.ONE
//							: TLNumericValue.ZERO);
//
//			return value;
//		}
//
//		@Override
//		public TLContext createContext() {
//			TLContext<TLValue> context = new TLContext<TLValue>();
//			context.setContext(null);
//			return context;
//		}
//
//		@Override
//		public TLType checkParameters(TLType[] parameters) {
//
//			TLType returnType = TLTypePrimitive.INTEGER;
//
//			if (parameters.length < minParams || parameters.length > maxParams) {
//				return TLType.ERROR;
//			}
//
//			if (!TLTypePrimitive.BOOLEAN.canAssign(parameters[0])) {
//				return TLType.ERROR;
//			}
//
//			if (parameters.length > 1) {
//
//				if (!parameters[1].isTypeSymbol()) {
//					return TLType.ERROR;
//				}
//
//				TLTypeSymbol typeSym = (TLTypeSymbol) parameters[1];
//				if (! typeSym.representsType()) {
//					return TLType.ERROR;
//				}
//				
//				returnType = typeSym.getRepresentedType();
//
//				if (!returnType.isNumeric()) {
//					return TLType.ERROR;
//				}
//			}
//
//			return TLType.OBJECT;
//		}
//	}
//
//	// STR2BOOL
//	public class Str2BoolFunction implements TLFunctionPrototype {
//
//		public Str2BoolFunction() {
//			super(
//					"convert",
//					"str2bool",
//					"Converts string to a boolean based on a pattern (i.e. \"true\")",
//					new TLType[][] { { TLTypePrimitive.STRING } },
//					TLTypePrimitive.BOOLEAN);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//
//			if (trueFormat.matches(params[0].toString()))
//				return TLBooleanValue.TRUE;
//
//			if (falseFormat.matches(params[0].toString()))
//				return TLBooleanValue.FALSE;
//
//			throw new TransformLangExecutorRuntimeException(params,
//					"str2bool - can't convert \"" + params[0] + "\" to "
//							+ TLValueType.BOOLEAN.getName());
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length < minParams || parameters.length > maxParams) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// if (parameters[0] != TLTypePrimitive.STRING) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// return TLTypePrimitive.BOOLEAN;
//		// }
//	}
//
//	// toString
//	public class ToStringFunction implements TLFunctionPrototype {
//
//		public ToStringFunction() {
//			super("convert", "to_string",
//					"Returns string representation of its argument",
//					new TLType[][] { { TLType.OBJECT } },
//					TLTypePrimitive.STRING);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLStringValue val = (TLStringValue) context.context;
//			val.setValue(params[0].toString());
//			return val;
//		}
//
//		@Override
//		public TLContext createContext() {
//			return TLContext.createStringContext();
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length != 1) {
//		// return TLType.ERROR;
//		// }
//		//
//		// return TLTypePrimitive.STRING;
//		// }
//	}
//
//	// Long2Date
//	public class Long2DateFunction implements TLFunctionPrototype {
//
//		public Long2DateFunction() {
//			super(
//					"convert",
//					"long2date",
//					"Returns date from long that represents milliseconds from epoch",
//					new TLType[][] { { TLTypePrimitive.LONG } },
//					TLTypePrimitive.DATETIME);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLValue val = (TLValue) context.getContext();
//			if (!(val instanceof TLDateValue)) {
//				val = TLValue.create(TLValueType.DATE);
//				context.setContext(val);
//			}
//			val.setValue(new Date(((TLNumericValue<CloverLong>) params[0])
//					.getLong()));
//			return val;
//		}
//
//		@Override
//		public TLContext createContext() {
//			return TLContext.createDateContext();
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length != 1) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// if (parameters[0] != TLTypePrimitive.LONG) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// return TLTypePrimitive.DATETIME;
//		// }
//	}
//
//	// DATE2LONG
//	public class Date2LongFunction implements TLFunctionPrototype {
//
//		public Date2LongFunction() {
//			super(
//					"convert",
//					"date2long",
//					"Returns long that represents milliseconds from epoch to a date",
//					new TLType[][] { { TLTypePrimitive.DATETIME } },
//					TLTypePrimitive.LONG);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLNumericValue value = (TLNumericValue) context.getContext();
//			if (value == null || !(value.type == TLValueType.LONG)) {
//				// initialize
//				value = (TLNumericValue) TLValue.create(TLValueType.LONG);
//				context.setContext(value);
//			}
//			value.setValue(((TLDateValue) params[0]).getDate().getTime());
//			return value;
//		}
//
//		@Override
//		public TLContext createContext() {
//			return TLContext.createLongContext();
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length != 1) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// if (parameters[0] != TLTypePrimitive.DATETIME) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// return TLTypePrimitive.LONG;
//		// }
//	}
//
//	// BASE64BYTE
//	public class Base64ByteFunction implements TLFunctionPrototype {
//
//		public Base64ByteFunction() {
//			super("convert", "base64byte",
//					"Converts binary data encoded in base64 to array of bytes",
//					new TLType[][] { { TLTypePrimitive.STRING } },
//					TLType.BYTEARRAY);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLByteArrayValue value = (TLByteArrayValue) context.getContext();
//			value.getByteAraray().decodeBase64(params[0].toString());
//			return value;
//		}
//
//		@Override
//		public TLContext createContext() {
//			return TLContext.createByteContext();
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length != 1) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// if (parameters[0] != TLTypePrimitive.STRING) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// return TLType.BYTEARRAY;
//		// }
//	}
//
//	// BYTE2BASE64
//	public class Byte2Base64Function implements TLFunctionPrototype {
//
//		public Byte2Base64Function() {
//			super("convert", "byte2base64",
//					"Converts binary data into their base64 representation",
//					new TLType[][] { { TLType.BYTEARRAY } },
//					TLTypePrimitive.STRING);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLStringValue value = (TLStringValue) context.getContext();
//			value.setValue(((TLByteArrayValue) params[0]).getByteAraray()
//					.encodeBase64());
//			return value;
//		}
//
//		@Override
//		public TLContext createContext() {
//			return TLContext.createStringContext();
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length != 1 ) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// if (parameters[0] != TLType.BYTEARRAY) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// return TLTypePrimitive.STRING;
//		// }
//	}
//
//	// BITS2STR
//	public class Bits2StrFunction implements TLFunctionPrototype {
//
//		public Bits2StrFunction() {
//			super("convert", "bits2str",
//					"Converts bits into their string representation",
//					new TLType[][] { { TLType.BYTEARRAY } },
//					TLTypePrimitive.STRING);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLStringValue value = (TLStringValue) context.getContext();
//			if (params[0].getType() != TLValueType.BYTE) {
//				throw new TransformLangExecutorRuntimeException(params,
//						"bits2str - can't convert \"" + params[0] + "\" to "
//								+ TLValueType.STRING.getName());
//			}
//			ByteArray bits = ((TLByteArrayValue) params[0]).getByteAraray();
//			value.setValue(bits
//					.decodeBitString('1', '0', 0, bits.length() << 3));
//			return value;
//		}
//
//		@Override
//		public TLContext createContext() {
//			return TLContext.createStringContext();
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length != 1) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// if (parameters[0] != TLType.BYTEARRAY) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// return TLTypePrimitive.STRING;
//		// }
//	}
//
//	// STR2BITS
//	public class Str2BitsFunction implements TLFunctionPrototype {
//
//		public Str2BitsFunction() {
//			super("convert", "str2bits",
//					"Converts string representation of bits into binary value",
//					new TLType[][] { { TLTypePrimitive.STRING } },
//					TLType.BYTEARRAY);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLByteArrayValue value = (TLByteArrayValue) context.getContext();
//			if (params[0].getType() != TLValueType.STRING) {
//				throw new TransformLangExecutorRuntimeException(params,
//						"str2bits - can't convert \"" + params[0] + "\" to "
//								+ TLValueType.BYTE.getName());
//			}
//			value.getByteAraray().encodeBitString((TLStringValue) params[0],
//					'1', true);
//			return value;
//		}
//
//		@Override
//		public TLContext createContext() {
//			return TLContext.createByteContext();
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length != 1) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// if (parameters[0] != TLTypePrimitive.STRING) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// return TLType.BYTEARRAY;
//		// }
//	}
//
//	// BYTE2HEX
//	public class Byte2HexFunction implements TLFunctionPrototype {
//
//		public Byte2HexFunction() {
//			super("convert", "byte2hex",
//					"Converts binary data into hex string",
//					new TLType[][] { { TLType.BYTEARRAY } },
//					TLTypePrimitive.STRING);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLStringValue value = (TLStringValue) context.getContext();
//			if (params[0].getType() != TLValueType.BYTE) {
//				throw new TransformLangExecutorRuntimeException(params,
//						"byte2hex - can't convert \"" + params[0] + "\" to "
//								+ TLValueType.STRING.getName());
//			}
//			StringBuilder strVal = (StringBuilder) value.getValue();
//			ByteArray bytes = ((TLByteArrayValue) params[0]).getByteAraray();
//			strVal.setLength(0);
//			strVal.ensureCapacity(bytes.length());
//			for (int i = 0; i < bytes.length(); i++) {
//				strVal.append(Character.forDigit(
//						(bytes.getByte(i) & 0xF0) >> 4, 16));
//				strVal.append(Character.forDigit(bytes.getByte(i) & 0x0F, 16));
//			}
//			return value;
//		}
//
//		@Override
//		public TLContext createContext() {
//			return TLContext.createStringContext();
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length != 1) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// if (parameters[0] != TLType.BYTEARRAY) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// return TLTypePrimitive.STRING;
//		// }
//	}
//
//	// HEX2BYTE
//	public class Hex2ByteFunction implements TLFunctionPrototype {
//
//		public Hex2ByteFunction() {
//			super("convert", "hex2byte", "Converts hex string into binary",
//					new TLType[][] { { TLTypePrimitive.STRING } },
//					TLType.BYTEARRAY);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLByteArrayValue value = (TLByteArrayValue) context.getContext();
//			if (params[0].getType() != TLValueType.STRING) {
//				throw new TransformLangExecutorRuntimeException(params,
//						"hex2byte - can't convert \"" + params[0] + "\" to "
//								+ TLValueType.BYTE.getName());
//			}
//			ByteArray bytes = value.getByteAraray();
//			bytes.reset();
//			CharSequence chars = (TLStringValue) params[0];
//			for (int i = 0; i < chars.length() - 1; i = i + 2) {
//				bytes.append((byte) (((byte) Character.digit(chars.charAt(i),
//						16) << 4) | (byte) Character.digit(chars.charAt(i + 1),
//						16)));
//			}
//			return value;
//		}
//
//		@Override
//		public TLContext createContext() {
//			return TLContext.createByteContext();
//		}
//
//		// @Override
//		// public TLType checkParameters(TLType[] parameters) {
//		// if (parameters.length != 1) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// if (parameters[0] != TLTypePrimitive.STRING) {
//		// return TLType.ERROR;
//		// }
//		//			
//		// return TLType.BYTEARRAY;
//		// }
//	}
//
//	// TRY_CONVERT
//	class TryConvertFunction implements TLFunctionPrototype {
//
//		public TryConvertFunction() {
//			super("convert", "try_convert",
//					"Tries to convert variable of one type to another",
//					new TLType[][] { { TLType.OBJECT }, { TLType.OBJECT } },
//					TLType.OBJECT, 3, 2);
//		}
//
//		@Override
//		public TLValue execute(TLValue[] params, TLContext context) {
//			TLValueType fromType = params[0].type;
//			TLValueType toType = params[1].type;
//
//			boolean canConvert = false;
//			if (fromType == toType) {
//				if (fromType == TLValueType.DECIMAL) {// check precision
//					TLValue candidate = TLValue.create(toType);
//					candidate.setValue(params[0].getNumeric());
//					canConvert = candidate.compareTo(params[0]) == 0;
//					if (!canConvert) {
//						return TLBooleanValue.FALSE;
//					}
//				}
//				params[1].setValue(params[0].getValue());
//				return TLBooleanValue.TRUE;
//			}
//
//			TLFunctionPrototype convertFunction = getConvertToFunction(
//					fromType, toType);
//			if (convertFunction == null) {
//				return TLBooleanValue.FALSE;
//			}
//
//			try {
//				params[1].setValue(convertFunction.execute(getConvertParams(
//						convertFunction, params), getConvertContext(
//						convertFunction, context)));
//			} catch (TransformLangExecutorRuntimeException e) {
//				return TLBooleanValue.FALSE;
//			}
//
//			return TLBooleanValue.TRUE;
//		}
//
//		@Override
//		public TLContext createContext() {
//			TLContext<TLValue> context = new TLContext<TLValue>();
//			context.setContext(null);
//			return context;
//		}
//
//		@Override
//		public TLType checkParameters(TLType[] parameters) {
//			if (parameters.length < minParams || parameters.length > maxParams) {
//				return TLType.ERROR;
//			}
//
//			return TLType.OBJECT;
//		}
//
//		private TLContext getConvertContext(TLFunctionPrototype function,
//				TLContext context) {
//			if ((function instanceof Num2StrFunction || function instanceof ToStringFunction)
//					&& !(context.getContext() instanceof TLStringValue)) {
//				return TLContext.createStringContext();
//			}
//			if (function instanceof Date2StrFunction
//					&& !(context.getContext() instanceof Date2StrContext)) {
//				return Date2StrContext.createContex();
//			}
//			if (function instanceof Str2DateFunction
//					&& !(context.getContext() instanceof Str2DateContext)) {
//				return Str2DateContext.createContext();
//			}
//			if (function instanceof Num2NumFunction
//					|| function instanceof Bool2NumFunction
//					|| function instanceof Date2LongFunction
//					&& !(context.getContext() instanceof TLNumericValue)) {
//				return TLContext.createNullContext();
//			}
//			if (function instanceof Long2DateFunction
//					&& !(context.getContext() instanceof TLDateValue)) {
//				return TLContext.createDateContext();
//			}
//			if (function instanceof Str2NumFunction
//					&& !(context.getContext() instanceof Str2NumContext)) {
//				return Str2NumContext.createContext();
//			}
//			return null;
//		}
//
//		private TLValue[] getConvertParams(TLFunctionPrototype function,
//				TLValue[] convertParams) {
//			if (function instanceof Num2StrFunction) {
//				return new TLValue[] {
//						convertParams[0],
//						new TLNumericValue<CloverInteger>(TLValueType.INTEGER,
//								new CloverInteger(DEFAULT_RADIX)) };
//			}
//			if (function instanceof Date2StrFunction
//					|| function instanceof Str2DateFunction) {
//				return new TLValue[] { convertParams[0], convertParams[2] };
//			}
//			if (function instanceof Num2NumFunction
//					|| function instanceof Bool2NumFunction) {
//				return new TLValue[] {
//						convertParams[0],
//						new TLNumericValue(TLValueType.SYM_CONST,
//								new CloverInteger(TLFunctionUtils
//										.valueType2astToken(convertParams[1]
//												.getType()))) };
//			}
//			if (function instanceof Str2NumFunction) {
//				TLValue[] result = new TLValue[convertParams.length];
//				result[0] = convertParams[0];
//				result[1] = new TLNumericValue(
//						TLValueType.SYM_CONST,
//						new CloverInteger(TLFunctionUtils
//								.valueType2astToken(convertParams[1].getType())));
//				if (result.length > 2) {
//					result[2] = convertParams[2];
//				}
//				return result;
//			}
//			return new TLValue[] { convertParams[0] };
//		}
//
//	}
//
//	private TLFunctionPrototype getConvertToFunction(TLValueType fromType,
//			TLValueType toType) {
//		if (fromType != TLValueType.BOOLEAN
//				&& toType != TLValueType.BOOLEAN
//				&& !(fromType.isCompatible(toType) || toType
//						.isCompatible(fromType))) {
//			return null;
//		}
//		switch (fromType) {
//		case INTEGER:
//		case LONG:
//		case NUMBER:
//		case DECIMAL:
//			switch (toType) {
//			case INTEGER:
//			case LONG:
//			case NUMBER:
//			case DECIMAL:
//				return new Num2NumFunction();
//			case BOOLEAN:
//				return new Num2BoolFunction();
//			case DATE:
//				return fromType == TLValueType.LONG ? new Long2DateFunction()
//						: null;
//			case BYTE:
//			case STRING:
//				return new Num2StrFunction();
//			default:
//				return null;
//			}
//		case BOOLEAN:
//			switch (toType) {
//			case INTEGER:
//			case LONG:
//			case NUMBER:
//			case DECIMAL:
//				return new Bool2NumFunction();
//			case BYTE:
//			case STRING:
//				return new ToStringFunction();
//			default:
//				return null;
//			}
//		case DATE:
//			switch (toType) {
//			case LONG:
//				return new Date2LongFunction();
//			case STRING:
//				return new Date2StrFunction();
//			default:
//				return null;
//			}
//		case STRING:
//			switch (toType) {
//			case INTEGER:
//			case LONG:
//			case NUMBER:
//			case DECIMAL:
//				return new Str2NumFunction();
//			case BOOLEAN:
//				return new Str2BoolFunction();
//			case DATE:
//				return new Str2DateFunction();
//			default:
//				return null;
//			}
//		default:
//			return new ToStringFunction();
//		}
//	}
//
//}
//
//class Date2StrContext {
//	TLValue value;
//	SimpleDateFormat format;
//
//	static TLContext createContex() {
//		Date2StrContext con = new Date2StrContext();
//		con.value = TLValue.create(TLValueType.STRING);
//		con.format = new SimpleDateFormat();
//
//		TLContext<Date2StrContext> context = new TLContext<Date2StrContext>();
//		context.setContext(con);
//
//		return context;
//	}
//}
//
//class Str2DateContext {
//	TLValue value;
//	SimpleDateFormat formatter;
//	ParsePosition position;
//	String locale;
//
//	public void init(String locale, String pattern) {
//		formatter = (SimpleDateFormat) MiscUtils.createFormatter(
//				DataFieldMetadata.DATE_FIELD, locale, pattern);
//		this.locale = locale;
//		position = new ParsePosition(0);
//	}
//
//	public void reset(String newLocale, String newPattern) {
//		if (!newLocale.equals(locale)) {
//			formatter = (SimpleDateFormat) MiscUtils.createFormatter(
//					DataFieldMetadata.DATE_FIELD, newLocale, newPattern);
//			this.locale = newLocale;
//		}
//		resetPattern(newPattern);
//	}
//
//	public void resetPattern(String newPattern) {
//		if (!newPattern.equals(formatter.toPattern())) {
//			formatter.applyPattern(newPattern);
//		}
//		position.setIndex(0);
//	}
//
//	public void setLenient(boolean lenient) {
//		formatter.setLenient(lenient);
//	}
//
//	static TLContext createContext() {
//		Str2DateContext con = new Str2DateContext();
//		con.value = TLValue.create(TLValueType.DATE);
//
//		TLContext<Str2DateContext> context = new TLContext<Str2DateContext>();
//		context.setContext(con);
//
//		return context;
//	}
//}
//
//class Str2NumContext {
//	TLValue value;
//	NumberFormat format;
//
//	public void init(String pattern, TLValueType type) {
//		if (pattern != null) {
//			switch (type) {
//			case DECIMAL:
//			case NUMBER:
//				format = new NumericFormat(pattern);
//				break;
//			case INTEGER:
//			case LONG:
//				format = new DecimalFormat(pattern);
//				break;
//			default:
//				throw new IllegalArgumentException(
//						"Str2NumContex can't be defined for " + type.getName());
//			}
//		}
//		if (type == TLValueType.DECIMAL) {
//			value = null;
//		} else if (value == null || value.type != type) {
//			value = TLValue.create(type);
//		}
//
//	}
//
//	public String toPattern() {
//		if (format != null) {
//			if (format instanceof NumericFormat) {
//				return ((NumericFormat) format).toPattern();
//			}
//			return ((DecimalFormat) format).toPattern();
//		}
//		return null;
//	}
//
//	public void reset(String newPattern, TLValueType newType) {
//		if (newType == TLValueType.DECIMAL || value == null
//				|| newType != value.type) {
//			init(newPattern, newType);
//		} else if (newPattern != null && !newPattern.equals(toPattern())) {
//			if (format == null) {
//				init(newPattern, newType);
//			} else if (format instanceof NumericFormat) {
//				((NumericFormat) format).applyPattern(newPattern);
//			} else {
//				((DecimalFormat) format).applyPattern(newPattern);
//			}
//		}
//	}
//
//	static TLContext createContext() {
//		Str2NumContext con = new Str2NumContext();
//		con.init(null, TLValueType.DECIMAL);
//
//		TLContext<Str2NumContext> context = new TLContext<Str2NumContext>();
//		context.setContext(con);
//
//		return context;
//	}
}
