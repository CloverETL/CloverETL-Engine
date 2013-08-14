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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.data.DateFieldEnum;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.StringFormat;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.PackedDecimal;
import org.jetel.util.crypto.Base64;
import org.jetel.util.crypto.Digest;
import org.jetel.util.crypto.Digest.DigestType;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.formatter.NumericFormatter;
import org.jetel.util.formatter.NumericFormatterFactory;
import org.jetel.util.string.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

public class ConvertLib extends TLFunctionLibrary {

	public static final int DEFAULT_RADIX = 10;
	public static final StringFormat trueFormat = StringFormat.create(Defaults.DEFAULT_REGEXP_TRUE_STRING);
	public static final StringFormat falseFormat = StringFormat.create(Defaults.DEFAULT_REGEXP_FALSE_STRING);
	
	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		TLFunctionPrototype ret = 
			"num2str".equals(functionName) ? new Num2StrFunction() :
			"date2str".equals(functionName) ? new Date2StrFunction() :
			"str2date".equals(functionName) ? new Str2DateFunction() :
			"date2num".equals(functionName) ? new Date2NumFunction() : 
			"str2integer".equals(functionName) ? new Str2IntegerFunction() :
			"str2long".equals(functionName) ? new Str2LongFunction() :
			"str2double".equals(functionName) ? new Str2DoubleFunction() :
			"str2decimal".equals(functionName) ? new Str2DecimalFunction() :
			"long2integer".equals(functionName) ? new Long2IntegerFunction() :
			"double2integer".equals(functionName) ? new Double2IntegerFunction() :
			"decimal2integer".equals(functionName) ? new Decimal2IntegerFunction() :
			"double2long".equals(functionName) ? new Double2LongFunction() :
			"decimal2long".equals(functionName) ? new Decimal2LongFunction() :
			"decimal2double".equals(functionName) ? new Decimal2DoubleFunction() : 
			"num2bool".equals(functionName) ? new Num2BoolFunction() :
			"bool2num".equals(functionName) ? new Bool2NumFunction() : 
			"str2bool".equals(functionName) ? new Str2BoolFunction() :
			"toString".equals(functionName) ? new ToStringFunction() :
			"long2date".equals(functionName) ? new Long2DateFunction() :
			"date2long".equals(functionName) ? new Date2LongFunction() : 
			"base64byte".equals(functionName) ? new Base64ByteFunction() : 
		    "byte2base64".equals(functionName) ? new Byte2Base64Function() : 
		    "bits2str".equals(functionName) ? new Bits2StrFunction() : 
		    "str2bits".equals(functionName) ? new Str2BitsFunction() : 
      	    "str2byte".equals(functionName) ? new Str2ByteFunction() : 
      	    "byte2str".equals(functionName) ? new Byte2StrFunction() : 
		    "hex2byte".equals(functionName) ? new Hex2ByteFunction() : 
		    "byte2hex".equals(functionName) ? new Byte2HexFunction() : 
		    "long2packDecimal".equals(functionName) ? new Long2PackedDecimalFunction() : 
		    "packDecimal2long".equals(functionName) ? new PackedDecimal2LongFunction() : 
			"xml2json".equals(functionName) ? new Xml2JsonFunction() : 
			"json2xml".equals(functionName) ? new Json2XmlFunction() : 
		    "md5".equals(functionName) ? new MD5Function() : 
		    "sha".equals(functionName) ? new SHAFunction() : 
			"sha256".equals(functionName) ? new SHA256Function() : 
		    "getFieldName".equals(functionName) ? new GetFieldNameFunction() : 
		    "getFieldType".equals(functionName) ? new GetFieldTypeFunction() : 
			null;
		
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
		
		return ret;
			
	}
	
	private static String LIBRARY_NAME = "Convert";

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}

		
	// NUM2STR
	@TLFunctionInitAnnotation
	public static final void num2strInit(TLFunctionCallContext context) {
		TLNumericFormatLocaleCache cache = new TLNumericFormatLocaleCache();
		//we have to use non-parametric constructor - in COMPILE mode we don't have context.getParams() available
		//TLNumericFormatLocaleCache cache = new TLNumericFormatLocaleCache(context.getParams()[0].isDecimal());
		cache.createCachedLocaleFormat(context, 1, 2);
		context.setCache(cache);
	}

	@TLFunctionAnnotation("Returns string representation of a number in a given format and locale")
	public static final String num2str(TLFunctionCallContext context, Integer num, String format, String locale) {
		//we have to do this for COMPILE mode - see note in num2strInit() method
		((TLNumericFormatLocaleCache)context.getCache()).setIsDecimal(false);
		NumericFormatter formatter = ((TLNumericFormatLocaleCache)context.getCache()).getCachedLocaleFormat(context, format, locale, 1, 2);
		return formatter.formatInt(num);
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given format")
	public static final String num2str(TLFunctionCallContext context, Integer num, String format) {
	    return num2str(context, num, format, MiscUtils.getDefautLocaleId());
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(TLFunctionCallContext context, Integer num, int radix) {
		return Integer.toString(num, radix);
	}
	@TLFunctionAnnotation("Returns string representation in decimal radix")
	public static final String num2str(TLFunctionCallContext context, Integer num) {
		return NumericFormatterFactory.getPlainFormatterInstance().formatInt(num);
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given format and locale")
	public static final String num2str(TLFunctionCallContext context, Long num, String format, String locale) {
		//we have to do this for COMPILE mode - see note in num2strInit() method
		((TLNumericFormatLocaleCache)context.getCache()).setIsDecimal(false);
		NumericFormatter formatter = ((TLNumericFormatLocaleCache)context.getCache()).getCachedLocaleFormat(context, format, locale, 1, 2);
		return formatter.formatLong(num);
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given format")
	public static final String num2str(TLFunctionCallContext context, Long num, String format) {
		return num2str(context, num, format, MiscUtils.getDefautLocaleId()); 	
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(TLFunctionCallContext context, Long num, int radix) {
		return Long.toString(num, radix);
	}
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(TLFunctionCallContext context, Long num) {
		return NumericFormatterFactory.getPlainFormatterInstance().formatLong(num);
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given format and locale")
	public static final String num2str(TLFunctionCallContext context, Double num, String format, String locale) {
		//we have to do this for COMPILE mode - see note in num2strInit() method
		((TLNumericFormatLocaleCache)context.getCache()).setIsDecimal(false);
		NumericFormatter formatter = ((TLNumericFormatLocaleCache)context.getCache()).getCachedLocaleFormat(context, format, locale, 1, 2);
		return formatter.formatDouble(num);
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given format")
	public static final String num2str(TLFunctionCallContext context, Double num, String format) {
	    return num2str(context, num, format, MiscUtils.getDefautLocaleId());
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(TLFunctionCallContext context, Double num, int radix) {
		switch (radix) {
		case 10:
			return Double.toString(num);
		case 16: 
			return Double.toHexString(num);
		default:
			throw new TransformLangExecutorRuntimeException("num2str for double type only supports radix 10 and 16");
		} 
	}
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(TLFunctionCallContext context, Double num) {
		return NumericFormatterFactory.getPlainFormatterInstance().formatDouble(num);
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given format and locale")
	public static final String num2str(TLFunctionCallContext context, BigDecimal num, String format, String locale) {
		//we have to do this for COMPILE mode - see note in num2strInit() method
		((TLNumericFormatLocaleCache)context.getCache()).setIsDecimal(true);
		NumericFormatter formatter = ((TLNumericFormatLocaleCache)context.getCache()).getCachedLocaleFormat(context, format, locale, 1, 2);
		return formatter.formatBigDecimal(num);
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given format")
	public static final String num2str(TLFunctionCallContext context, BigDecimal num, String format) {
	    return num2str(context, num, format, MiscUtils.getDefautLocaleId());
	}
	
	@TLFunctionAnnotation("Returns string representation of a number in a given numeral system")
	public static final String num2str(TLFunctionCallContext context, BigDecimal num) {
		return NumericFormatterFactory.getPlainFormatterInstance().formatBigDecimal(num);
	}
	class Num2StrFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
			num2strInit(context);
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length > 1 && context.getParams()[1].isString()) {
				String locale;
				if (context.getParams().length == 3) {
					locale = stack.popString(); 
				} else {
					locale = MiscUtils.getDefautLocaleId();
				}
				String format = stack.popString();
				if (context.getParams()[0].isInteger()) {
					stack.push(num2str(context, stack.popInt(), format, locale));
				} else if (context.getParams()[0].isLong()) {
					stack.push(num2str(context, stack.popLong(), format, locale));
				} else if (context.getParams()[0].isDouble()) {
					stack.push(num2str(context, stack.popDouble(), format, locale));
				} else if (context.getParams()[0].isDecimal()) {
					stack.push(num2str(context, stack.popDecimal(), format, locale));
				}
			} else {
				int radix = 10;
				if (context.getParams().length > 1) {
					radix = stack.popInt();
				}
				if (context.getParams()[0].isInteger()) {
					stack.push(num2str(context, stack.popInt(),radix));
				} else if (context.getParams()[0].isLong()) {
					stack.push(num2str(context, stack.popLong(),radix));
				} else if (context.getParams()[0].isDouble()) {
					stack.push(num2str(context, stack.popDouble(),radix));
				} else if (context.getParams()[0].isDecimal()) {
					stack.push(num2str(context, stack.popDecimal()));
				}
			}
		}
	}

	// DATE2STR
	class Date2StrFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
			date2strInit(context);
		}
	
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String locale = null;
			String timeZone = null;
			
			if (context.getParams().length > 3) {
				timeZone = stack.popString();
			}
			if (context.getParams().length > 2) {
				locale = stack.popString();
			}

			final String pattern = stack.popString();
			final Date date = stack.popDate();
			stack.push(date2str(context, date, pattern, locale, timeZone));
		}
	}

	@TLFunctionInitAnnotation
	public static final void date2strInit(TLFunctionCallContext context) {
		context.setCache(new TLDateFormatLocaleCache(context, 1, 2, 3));
	}
	
	@TLFunctionAnnotation("Converts date to string according to the specified pattern.")
	public static final String date2str(TLFunctionCallContext context, Date date, String pattern) {
		return date2str(context, date, pattern, null);
	}

	@TLFunctionAnnotation("Converts date to string according to the specified pattern and locale.")
	public static final String date2str(TLFunctionCallContext context, Date date, String pattern, String locale) {
		return date2str(context, date, pattern, locale, null);
	}

	@TLFunctionAnnotation("Converts date to string according to the specified pattern, locale and time zone.")
	public static final String date2str(TLFunctionCallContext context, Date date, String pattern, String locale, String timeZone) {
		if (date == null){
			return null;
		}
		final DateFormatter formatter = ((TLDateFormatLocaleCache) context.getCache()).getCachedLocaleFormatter(context, pattern, locale, timeZone, 1, 2, 3);
		return formatter.format(date);
	}
	
	// STR2DATE
	class Str2DateFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
			str2dateInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String locale = null;
			String timeZone = null;
			
			if (context.getParams().length > 3) {
				timeZone = stack.popString();
			}
			if (context.getParams().length > 2) {
				
				locale = stack.popString();
			}
			
			final String pattern = stack.popString();
			final String input = stack.popString();
		
			stack.push(str2date(context, input, pattern, locale, timeZone));
		}
	}

	@TLFunctionInitAnnotation
	public static final void str2dateInit(TLFunctionCallContext context) {
		context.setCache(new TLDateFormatLocaleCache(context, 1, 2, 3));
	}
	
	@TLFunctionAnnotation("Converts string to date using the specified pattern, locale and time zone")
	public static final Date str2date(TLFunctionCallContext context, String input, String pattern, String locale, String timeZone) {
		DateFormatter formatter = ((TLDateFormatLocaleCache) context.getCache()).getCachedLocaleFormatter(context, pattern, locale, timeZone, 1, 2, 3);
		return formatter.parseDate(input);
	}

	@TLFunctionAnnotation("Converts string to date using the specified pattern and locale")
	public static final Date str2date(TLFunctionCallContext context, String input, String pattern, String locale) {
		return str2date(context, input, pattern, null, null);
	}

	@TLFunctionAnnotation("Converts string to date using the specified pattern")
	public static final Date str2date(TLFunctionCallContext context, String input, String pattern) {
		return str2date(context, input, pattern, null);
	}

	// DATE2NUM
	class Date2NumFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
			date2numInit(context);
		}


		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length > 2) {
				final String locale = stack.popString();
				final DateFieldEnum field = (DateFieldEnum)stack.pop();
				final Date input = stack.popDate();
				stack.push(date2num(context, input, field, locale));
			} else {
				final DateFieldEnum field = (DateFieldEnum)stack.pop();
				final Date input = stack.popDate();
				stack.push(date2num(context, input,field));
			}
		}
	}

	@TLFunctionInitAnnotation
	public static final void date2numInit(TLFunctionCallContext context) {
		context.setCache(new TLCalendarCache(context, 2));
	}
	
	@TLFunctionAnnotation("Returns numeric value of a date component (e.g. month)")
	public static final Integer date2num(TLFunctionCallContext context, Date input, DateFieldEnum field) {
		Calendar c = ((TLCalendarCache)context.getCache()).getCalendar();
		return date2numInternal(input, field, c);
	}
	
	@TLFunctionAnnotation("Returns numeric value of a date component (e.g. month)")
	public static final Integer date2num(TLFunctionCallContext context, Date input, DateFieldEnum field, String locale) {
		Calendar c = ((TLCalendarCache)context.getCache()).getCachedCalendar(context, locale, 2);
		return date2numInternal(input, field, c);
	}
	
	private static final Integer date2numInternal(Date input, DateFieldEnum field, Calendar c){
		if (input == null){
			return null;
		}
		c.setTime(input);
		switch (field) {
		case YEAR:
			return c.get(Calendar.YEAR);
		case MONTH:
			return c.get(Calendar.MONTH) + 1; //months should be numerated from 1, not 0.
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
	
	@TLFunctionInitAnnotation
	public static final void str2integerInit(TLFunctionCallContext context) {
		TLNumericFormatLocaleCache cache = new TLNumericFormatLocaleCache(false);
		cache.createCachedLocaleFormat(context, 1, 2);
		context.setCache(cache);
	}
	
	@TLFunctionAnnotation("Parses string in given format and locale to integer.")
	public static final Integer str2integer(TLFunctionCallContext context, String input, String format, String locale) {
		NumericFormatter formatter = ((TLNumericFormatLocaleCache)context.getCache()).getCachedLocaleFormat(context, format, locale, 1, 2);

		try {
			return formatter.parseInt(input);
		} catch (ParseException e) {
			throw new TransformLangExecutorRuntimeException("str2integer - can't convert \"" + input + "\" " + 
					"with format \"" + format +  "\"" + (locale != null ? " and locale \"" + locale + "\"" : ""), e);
		}
	}
	
	@TLFunctionAnnotation("Parses string in given format to integer.")
	public static final Integer str2integer(TLFunctionCallContext context, String input, String format) {
		return str2integer(context, input, format, null);
	}
	
	@TLFunctionAnnotation("Parses string to integer using specific numeral system.")
	public static final Integer str2integer(TLFunctionCallContext context, String input, Integer radix) {
		return Integer.valueOf(input,radix);
	}
	
	@TLFunctionAnnotation("Parses string to integer.")
	public static final Integer str2integer(TLFunctionCallContext context, String input) {
		try {
			return NumericFormatterFactory.getPlainFormatterInstance().parseInt(input);
		} catch (ParseException e) {
			throw new TransformLangExecutorRuntimeException("str2integer - can't convert \"" + input + "\"", e);
		}
	}
	class Str2IntegerFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
			str2integerInit(context);
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length > 1 && context.getParams()[1].isString()) {
				String locale = null;
				if (context.getParams().length == 3) {
					locale = stack.popString(); 
				}
				String format = stack.popString();
				final String input = stack.popString();
				stack.push(str2integer(context, input, format, locale));
			} else {
				int radix = 10;
				if (context.getParams().length == 2) {
					radix = stack.popInt();
				}
				final String input = stack.popString();
				stack.push(str2integer(context, input,radix));
			}
		}
	}

	@TLFunctionInitAnnotation
	public static final void str2longInit(TLFunctionCallContext context) {
		TLNumericFormatLocaleCache cache = new TLNumericFormatLocaleCache(false);
		cache.createCachedLocaleFormat(context, 1, 2);
		context.setCache(cache);
	}
	
	@TLFunctionAnnotation("Parses string in given format and locale to long.")
	public static final Long str2long(TLFunctionCallContext context, String input, String format, String locale) {
		NumericFormatter formatter = ((TLNumericFormatLocaleCache)context.getCache()).getCachedLocaleFormat(context, format, locale, 1, 2);

		try {
			return formatter.parseLong(input);
		} catch (ParseException e) {
			throw new TransformLangExecutorRuntimeException("str2long - can't convert \"" + input + "\" " + 
					"with format \"" + format +  "\"" + (locale != null ? " and locale \"" + locale + "\"" : ""), e);
		}
	}
	
	@TLFunctionAnnotation("Parses string in given format to long.")
	public static final Long str2long(TLFunctionCallContext context, String input, String format) {
		return str2long(context, input, format, null);
	}
	
	@TLFunctionAnnotation("Parses string to long using specific numeral system.")
	public static final Long str2long(TLFunctionCallContext context, String input, Integer radix) {
		return Long.valueOf(input,radix);
	}
	@TLFunctionAnnotation("Parses string to long using specific numeral system.")
	public static final Long str2long(TLFunctionCallContext context, String input) {
		try {
			return NumericFormatterFactory.getPlainFormatterInstance().parseLong(input);
		} catch (ParseException e) {
			throw new TransformLangExecutorRuntimeException("str2long - can't convert \"" + input + "\"", e);
		}
	}
	class Str2LongFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
			str2longInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length > 1 && context.getParams()[1].isString()) {
				String locale = null;
				if (context.getParams().length == 3) {
					locale = stack.popString(); 
				}
				String format = stack.popString();
				final String input = stack.popString();
				stack.push(str2long(context, input, format, locale));
			} else {
				int radix = 10;
				if (context.getParams().length == 2) {
					radix = stack.popInt();
				}
				final String input = stack.popString();
				stack.push(str2long(context, input,radix));
			}
		}
	}
	
	@TLFunctionInitAnnotation
	public static final void str2doubleInit(TLFunctionCallContext context) {
		TLNumericFormatLocaleCache cache = new TLNumericFormatLocaleCache(false);
		cache.createCachedLocaleFormat(context, 1, 2);
		context.setCache(cache);
	}
	
	@TLFunctionAnnotation("Parses string in given format and locale to double.")
	public static final Double str2double(TLFunctionCallContext context, String input, String format, String locale) {
		NumericFormatter formatter = ((TLNumericFormatLocaleCache)context.getCache()).getCachedLocaleFormat(context, format, locale, 1, 2);

		try {
			return formatter.parseDouble(input);
		} catch (ParseException e) {
			throw new TransformLangExecutorRuntimeException("str2double - can't convert \"" + input + "\" " + 
					"with format \"" + format +  "\"" + (locale != null ? " and locale \"" + locale + "\"" : ""), e);
		}
	}
	
	@TLFunctionAnnotation("Parses string in given format to double.")
	public static final Double str2double(TLFunctionCallContext context, String input, String format) {
		return str2double(context, input, format, null);
	}

	@TLFunctionAnnotation("Parses string to double using specific numeral system.")
	public static final Double str2double(TLFunctionCallContext context, String input) {
		try {
			return NumericFormatterFactory.getPlainFormatterInstance().parseDouble(input);
		} catch (ParseException e) {
			throw new TransformLangExecutorRuntimeException("str2double - can't convert \"" + input + "\"", e);
		}
	}
	class Str2DoubleFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
			str2doubleInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length > 1 && context.getParams()[1].isString()) {
				String locale = null;
				if (context.getParams().length == 3) {
					locale = stack.popString(); 
				}
				String format = stack.popString();
				final String input = stack.popString();
				stack.push(str2double(context, input, format, locale));
			} else {
				final String input = stack.popString();
				stack.push(str2double(context, input));
			}
		}
	}
	
	@TLFunctionInitAnnotation
	public static final void str2decimalInit(TLFunctionCallContext context) {
		TLNumericFormatLocaleCache cache = new TLNumericFormatLocaleCache(true);
		cache.createCachedLocaleFormat(context, 1, 2);
		context.setCache(cache);
	}
	
	@TLFunctionAnnotation("Parses string in given format and locale to decimal.")
	public static final BigDecimal str2decimal(TLFunctionCallContext context, String input, String format, String locale) {
		NumericFormatter formatter = ((TLNumericFormatLocaleCache)context.getCache()).getCachedLocaleFormat(context, format, locale, 1, 2);

		try {
			return formatter.parseBigDecimal(input);
		} catch (Exception e) {
			throw new JetelRuntimeException("can't convert \"" + input + "\" " + 
					"with format \"" + format +  "\"" + (locale != null ? " and locale \"" + locale + "\"" : ""), e);
		}
	}
	
	@TLFunctionAnnotation("Parses string in given format to decimal.")
	public static final BigDecimal str2decimal(TLFunctionCallContext context, String input, String format) {
		return str2decimal(context, input, format, null);
	}
	
	@TLFunctionAnnotation("Parses string to decimal.")
	public static final BigDecimal str2decimal(TLFunctionCallContext context, String input) {
		try {
			return NumericFormatterFactory.getPlainFormatterInstance().parseBigDecimal(input);
		} catch (Exception e) {
			throw new JetelRuntimeException("can't convert \"" + input + "\" to decimal", e);
		}
	}
	class Str2DecimalFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
			str2decimalInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length > 1 && context.getParams()[1].isString()) {
				String locale;
				if (context.getParams().length == 3) {
					locale = stack.popString(); 
				} else {
					locale = MiscUtils.getDefautLocaleId();
				}
				String format = stack.popString();
				final String input = stack.popString();
				stack.push(str2decimal(context, input, format, locale));
			} else {
				final String input = stack.popString();
				stack.push(str2decimal(context, input));
			}
		}
	}

	@TLFunctionAnnotation("Narrowing conversion from long to integer value.")
	public static final Integer long2integer(TLFunctionCallContext context, Long l) {
		if (l > Integer.MAX_VALUE || l <= Integer.MIN_VALUE) {
			throw new TransformLangExecutorRuntimeException("long2integer: " + l + " - out of range of integer");
		}
		return l.intValue();
		
	}
	
	class Long2IntegerFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(long2integer(context, stack.popLong()));
		}
	}
	
	@TLFunctionAnnotation("Narrowing conversion from double to integer value.")
	public static final Integer double2integer(TLFunctionCallContext context, Double l) {
		if (l == null){
			return null;
		}
		if (l > Integer.MAX_VALUE || l <= Integer.MIN_VALUE) {
			throw new TransformLangExecutorRuntimeException("double2integer: " + l + " - out of range of integer");
		}
		return l.intValue();
	}
	class Double2IntegerFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(double2integer(context, stack.popDouble()));
		}
	}
	
	public static final BigDecimal maxIntDecimal = new BigDecimal(Integer.MAX_VALUE);
	public static final BigDecimal minIntDecimal = new BigDecimal(Integer.MIN_VALUE);
	
	@TLFunctionAnnotation("Narrowing conversion from decimal to integer value.")
	public static final Integer decimal2integer(TLFunctionCallContext context, BigDecimal l) {
		if (l == null){
			return null;
		}
		if (l.compareTo(maxIntDecimal) > 0 || l.compareTo(minIntDecimal) <= 0) {
			throw new TransformLangExecutorRuntimeException("decimal2integer: " + l + " - out of range of integer");
		}
		return l.intValue();
	}
	class Decimal2IntegerFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(decimal2integer(context, stack.popDecimal()));
		}
	}
	
	@TLFunctionAnnotation("Narrowing conversion from double to long value.")
	public static final Long double2long(TLFunctionCallContext context, Double d) {
		if (d ==null){
			return null;
		}
		if (d > Long.MAX_VALUE || d <= Long.MIN_VALUE) {
			throw new TransformLangExecutorRuntimeException("double2long: " + d + " - out of range of long");
		}
		return d.longValue();
	}
	
	class Double2LongFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(double2long(context, stack.popDouble()));
		}
	}
	
	public static final BigDecimal maxLongDecimal = new BigDecimal(Long.MAX_VALUE);
	public static final BigDecimal minLongDecimal = new BigDecimal(Long.MIN_VALUE);
	
	@TLFunctionAnnotation("Narrowing conversion from decimal to long value.")
	public static final Long decimal2long(TLFunctionCallContext context, BigDecimal d) {
		if (d == null){
			return null;
		}
		if (d.compareTo(maxLongDecimal) > 0 || d.compareTo(minLongDecimal) <= 0) {
			throw new TransformLangExecutorRuntimeException("decimal2long: " + d + " - out of range of long");
		}
		return d.longValue();
	}

	class Decimal2LongFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(decimal2long(context, stack.popDecimal()));
		}
	}
	
	
	public static final BigDecimal maxDoubleDecimal = new BigDecimal(Double.MAX_VALUE, TransformLangExecutor.MAX_PRECISION);
	public static final BigDecimal minDoubleDecimal = new BigDecimal(-Double.MAX_VALUE, TransformLangExecutor.MAX_PRECISION);

	@TLFunctionAnnotation("Narrowing conversion from decimal to double value.")
	public static final Double decimal2double(TLFunctionCallContext context, BigDecimal d) {
		if (d == null){
			return null;
		}
		if (d.compareTo(maxDoubleDecimal) > 0 || d.compareTo(minDoubleDecimal) < 0) {
			throw new TransformLangExecutorRuntimeException("decimal2double: " + d + " - out of range of double");
		}
		return d.doubleValue();
	}

	class Decimal2DoubleFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(decimal2double(context, stack.popDecimal()));
		}
	}

	// NUM2BOOL
	@TLFunctionAnnotation("Converts 0 to false and any other numeric value to true.")
	public static final Boolean num2bool(TLFunctionCallContext context, int b) {
		return b != 0;
	}
	
	@TLFunctionAnnotation("Converts 0 to false and any other numeric value to true.")
	public static final Boolean num2bool(TLFunctionCallContext context, long b) {
		return b != 0;
	}
	
	@TLFunctionAnnotation("Converts 0 to false and any other numeric value to true.")
	public static final Boolean num2bool(TLFunctionCallContext context, double b) {
		return b != 0;
	}
	
	@TLFunctionAnnotation("Converts 0 to false and any other numeric value to true.")
	public static final Boolean num2bool(TLFunctionCallContext context, BigDecimal b) {
		return BigDecimal.ZERO.compareTo(b) != 0;
	}

	class Num2BoolFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isInteger()) {
				stack.push(num2bool(context, stack.popInt()));
			} else if (context.getParams()[0].isLong()) {
				stack.push(num2bool(context, stack.popLong()));
			} else if (context.getParams()[0].isDouble()) {
				stack.push(num2bool(context, stack.popDouble()));
			} else if (context.getParams()[0].isDecimal()) {
				stack.push(num2bool(context, stack.popDecimal()));
			}
		}
		
	}

	
	@TLFunctionAnnotation("Converts true to 1 and false to 0.")
	public static final Integer bool2num(TLFunctionCallContext context, boolean b) {
		return b ? 1 : 0;
	}
	
	// BOOL2NUM
	class Bool2NumFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(bool2num(context, stack.popBoolean()));
		}

	}

	@TLFunctionAnnotation("Converts string to a boolean based on a pattern (i.e. \"true\")")
	public static final Boolean str2bool(TLFunctionCallContext context, String s) {
		if (trueFormat.matches(s)) 
			return Boolean.TRUE;
		
		if (falseFormat.matches(s)) 
			return Boolean.FALSE;
		
		throw new TransformLangExecutorRuntimeException("str2bool - can't convert \"" + s + "\" to boolean");
	}
	
	// STR2BOOL
	class Str2BoolFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(str2bool(context, stack.popString()));
		}
	}
	
	// this method is not annotated as it should not be directly visible in CTL
	private static final String toStringInternal(Object o) {
		return StringUtils.toOutputStringCTL(o);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final String toString(TLFunctionCallContext context, int i) {
		return toStringInternal(i);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final String toString(TLFunctionCallContext context, long l) {
		return toStringInternal(l);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final String toString(TLFunctionCallContext context, double d) {
		return toStringInternal(d);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final String toString(TLFunctionCallContext context, BigDecimal d) {
		return toStringInternal(d);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final <E> String toString(TLFunctionCallContext context, List<E> list) {
		return toStringInternal(list);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final <K,V> String toString(TLFunctionCallContext context, Map<K,V> map) {
		return toStringInternal(map);
	}
	
	// toString
	class ToStringFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(toStringInternal(stack.pop()));
		}

	}

	
	@TLFunctionAnnotation("Returns date from long that represents milliseconds from epoch")
	public static final Date long2date(TLFunctionCallContext context, Long l) {
		return new Date(l);
	}
	
	// Long2Date
	class Long2DateFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(long2date(context, stack.popLong()));
		}

	}

	
	@TLFunctionAnnotation("Returns long that represents milliseconds from epoch to a date")
	public static final Long date2long(TLFunctionCallContext context, Date d) {
		if (d == null){
			return null;
		}
		return d.getTime();
	}
	
	// DATE2LONG
	class Date2LongFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(date2long(context, stack.popDate()));
		}

	}
	
	@TLFunctionAnnotation("Converts binary data encoded in base64 to array of bytes.")
	public static final byte[] base64byte(TLFunctionCallContext context, String src) {
		return Base64.decode(src);
	}	
	
	// BASE64BYTE
	public class Base64ByteFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(base64byte(context, stack.popString()));
		}
	}
	
	@TLFunctionAnnotation("Converts binary data into their base64 representation.")
	public static final String byte2base64(TLFunctionCallContext context, byte[] src) {
		return Base64.encodeBytes(src);
	}
	
	// BYTE2BASE64
	public class Byte2Base64Function implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(byte2base64(context, stack.popByteArray()));
		}
	}
	
	@TLFunctionAnnotation("Converts bits into their string representation.")
	public static final String bits2str(TLFunctionCallContext context, byte[] src) {
		StringBuilder sb = new StringBuilder();
		for (int i=0; i < src.length << 3; i++) {
			sb.append((src[i >> 3] & (1 << (i & 7))) != 0 ? '1' : '0');
		}
		return sb.toString();
	}

	// BITS2STR
	public class Bits2StrFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(bits2str(context, stack.popByteArray()));
		}
	}
	
	@TLFunctionAnnotation("Converts string representation of bits into binary value.")
	public static final byte[] str2bits(TLFunctionCallContext context, String src) {
		byte[] bits = new byte[(src.length() >> 3) + ((src.length() & 7) != 0 ? 1 : 0)];
		for (int i = 0; i < src.length(); i++) {
			if (src.charAt(i) == '1') {
				bits[i >> 3] |= (1 << (i & 7));
			}
		}
		return bits;
	}

	// STR2BITS
	public class Str2BitsFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(str2bits(context, stack.popString()));
		}
	}

	@TLFunctionAnnotation("Converts string to byte according to given charset")
	public static final byte[] str2byte(TLFunctionCallContext context, String src, String charset) {
		try {
			return src.getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			throw new TransformLangExecutorRuntimeException("str2byte - can't convert \"" + src + "\" " +
					"with charset \"" + charset + ": unknown charset");

		}
	}

	// STR2BYTE
	public class Str2ByteFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String charset = stack.popString();
			String src = stack.popString();
			stack.push(str2byte(context, src, charset));
		}
	}

	@TLFunctionAnnotation("Converts byte to string according to given charset")
	public static final String byte2str(TLFunctionCallContext context, byte[] src, String charset) {
		try {
			return new String(src, charset);
		} catch (UnsupportedEncodingException e) {
			throw new TransformLangExecutorRuntimeException("byte2str - can't convert \"" + Arrays.toString(src) + "\" " +
					"with charset \"" + charset + ": unknown charset");

		}
	}

	// BYTE2STR
	public class Byte2StrFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String charset = stack.popString();
			byte[] src = stack.popByteArray();
			stack.push(byte2str(context, src, charset));
		}
	}

	@TLFunctionAnnotation("Converts binary data into hex string.")
	public static final String byte2hex(TLFunctionCallContext context, byte[] src) {
		return StringUtils.bytesToHexString(src);
	}
	
	// BYTE2HEX
	public class Byte2HexFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(byte2hex(context, stack.popByteArray()));
		}
	}
	
	@TLFunctionAnnotation("Converts hex string into binary.")
	public static final byte[] hex2byte(TLFunctionCallContext context, String src) {
		return StringUtils.hexStringToBytes(src);
	}

	// HEX2BYTE
	public class Hex2ByteFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(hex2byte(context, stack.popString()));
		}
	}
	
	@TLFunctionAnnotation("Converts long into packed decimal representation (bytes).")
	public static final byte[] long2packDecimal(TLFunctionCallContext context, Long src) {
		byte[] tmp = new byte[16];
		int length = PackedDecimal.format(src, tmp);
		byte[] result = new byte[length];
		System.arraycopy(tmp, 0, result, 0, length);
		return result;
	}
	
	// LONG2PACKEDDECIMAL
	class Long2PackedDecimalFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(long2packDecimal(context, stack.popLong()));
		}
	}
	
	@TLFunctionAnnotation("Converts packed decimal(bytes) into long value.")
	public static final Long packDecimal2long(TLFunctionCallContext context, byte[] array) {
		return PackedDecimal.parse(array);
	}
	
	// PACKEDDECIMAL2LONG
	class PackedDecimal2LongFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(packDecimal2long(context, stack.popByteArray()));
		}
	}

	@TLFunctionAnnotation("Returns json representation of a xml text")
	public static final String xml2json(TLFunctionCallContext contex, String xml){
		try {
			return XML.toJSONObject(xml).toString();
		} catch (JSONException e) {
			throw new TransformLangExecutorRuntimeException("xml2json - can't convert \"" + xml + "\"", e);
		}
	}
	
	//XML2JSON
	class Xml2JsonFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String input = stack.popString();
			stack.push(xml2json(context, input));
		}

	}

	@TLFunctionAnnotation("Returns xml representation of a json object")
	public static final String json2xml(TLFunctionCallContext contex, String json){
		try {
			return XML.toString(new JSONObject(json));
		} catch (JSONException e) {
			throw new TransformLangExecutorRuntimeException("json2xml - can't convert \"" + json + "\"", e);
		}
	}
	
	//JSON2XML
	class Json2XmlFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String input = stack.popString();
			stack.push(json2xml(context, input));
		}

	}

	@TLFunctionAnnotation("Calculates MD5 hash of input string.")
	public static final byte[] md5(TLFunctionCallContext context, String src) {
		return Digest.digest(DigestType.MD5, src);
	}
	
	@TLFunctionAnnotation("Calculates MD5 hash of input bytes.")
	public static final byte[] md5(TLFunctionCallContext context, byte[] src) {
		return Digest.digest(DigestType.MD5, src);
	}
	
	// MD5
	class MD5Function implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if(context.getParams()[0].isString()) {
				stack.push(md5(context, stack.popString()));
			} else {
				stack.push(md5(context, stack.popByteArray()));
			}
		}
	}
	
	@TLFunctionAnnotation("Calculates SHA hash of input bytes.")
	public static final byte[] sha(TLFunctionCallContext context, byte[] src) {
		return Digest.digest(DigestType.SHA, src);
	}
	
	@TLFunctionAnnotation("Calculates SHA hash of input string.")
	public static final byte[] sha(TLFunctionCallContext context, String src) {
		return Digest.digest(DigestType.SHA, src);
	}
	
	// SHA
	class SHAFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if(context.getParams()[0].isString()) {
				stack.push(sha(context, stack.popString()));
			} else {
				stack.push(sha(context, stack.popByteArray()));
			}
		}
	}

	@TLFunctionAnnotation("Calculates SHA-256 hash of input bytes.")
	public static final byte[] sha256(TLFunctionCallContext context, byte[] src) {
		return Digest.digest(DigestType.SHA256, src);
	}
	
	@TLFunctionAnnotation("Calculates SHA-256 hash of input string.")
	public static final byte[] sha256(TLFunctionCallContext context, String src) {
		return Digest.digest(DigestType.SHA256, src);
	}
	
	// SHA-256
	class SHA256Function implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if(context.getParams()[0].isString()) {
				stack.push(sha256(context, stack.popString()));
			} else {
				stack.push(sha256(context, stack.popByteArray()));
			}
		}
	}

	@TLFunctionAnnotation("Returns name of i-th field of passed-in record.")
	public static final String getFieldName(TLFunctionCallContext context, DataRecord record, Integer position) {
		if (position < 0 || position >= record.getNumFields()) {
			throw new JetelRuntimeException("field with index " + position + " does not exist in metadata '" + record.getMetadata().getName() + "'");
		}
		return record.getField(position).getMetadata().getName();
	}
	
	//GETFIELDNAME
	class GetFieldNameFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			Integer position = stack.popInt();
			DataRecord record = stack.popRecord();
			stack.push(getFieldName(context, record, position));
		}
	}
	
	@TLFunctionAnnotation("Returns data type of i-th field of passed-in record")
	public static final String getFieldType(TLFunctionCallContext context, DataRecord record, Integer position) {
		if (position < 0 || position >= record.getNumFields()) {
			throw new JetelRuntimeException("field with index " + position + " does not exist in metadata '" + record.getMetadata().getName() + "'");
		}
		return record.getField(position).getMetadata().getDataType().getName();
	}

	//GETFIELDTYPE
	class GetFieldTypeFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			Integer position = stack.popInt();
			DataRecord record = stack.popRecord();
			stack.push(getFieldType(context, record, position));
		}
	}
}
