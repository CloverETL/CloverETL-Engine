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
import java.util.List;
import java.util.Map;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.data.DateFieldEnum;
import org.jetel.ctl.data.TLType;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.ByteArray;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.PackedDecimal;
import org.jetel.util.crypto.Digest;
import org.jetel.util.crypto.Digest.DigestType;

public class ConvertLib extends TLFunctionLibrary {

	private static final String LIBRARY_NAME = "Convert";

	public static final int DEFAULT_RADIX = 10;

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
			"long2int".equals(functionName) ? new Long2IntFunction() :
			"double2int".equals(functionName) ? new Double2IntFunction() :
			"decimal2int".equals(functionName) ? new Decimal2IntFunction() :
			"double2long".equals(functionName) ? new Double2LongFunction() :
			"decimal2long".equals(functionName) ? new Decimal2LongFunction() :
			"decimal2double".equals(functionName) ? new Decimal2DoubleFunction() : 
			"num2bool".equals(functionName) ? new Num2BoolFunction() :
			"bool2num".equals(functionName) ? new Bool2NumFunction() : 
			"str2bool".equals(functionName) ? new Str2BoolFunction() :
			"to_string".equals(functionName) ? new ToStringFunction() :
			"long2date".equals(functionName) ? new Long2DateFunction() :
			"date2long".equals(functionName) ? new Date2LongFunction() : 
			"base64byte".equals(functionName) ? new Base64ByteFunction() : 
		    "byte2base64".equals(functionName) ? new Byte2Base64Function() : 
		    "bits2str".equals(functionName) ? new Bits2StrFunction() : 
		    "str2bits".equals(functionName) ? new Str2BitsFunction() : 
		    "hex2byte".equals(functionName) ? new Hex2ByteFunction() : 
		    "byte2hex".equals(functionName) ? new Byte2HexFunction() : 
		    "long2pacdecimal".equals(functionName) ? new Long2PackedDecimalFunction() : 
		    "packdecimal2long".equals(functionName) ? new PackedDecimal2LongFunction() : 
		    "md5".equals(functionName) ? new MD5Function() : 
		    "sha".equals(functionName) ? new SHAFunction() : 
		    "get_field_name".equals(functionName) ? new GetFieldNameFunction() : 
		    "get_field_type".equals(functionName) ? new GetFieldTypeFunction() : 
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
			throw new TransformLangExecutorRuntimeException("num2str for double type only supports radix 10 and 16");
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

	// DATE2STR
	class Date2StrFunction implements TLFunctionPrototype {
	
		public void execute(Stack stack, TLType[] actualParams) {
			final String pattern = stack.popString();
			final Date date = stack.popDate();
			stack.push(date2str(date,pattern));
		}
	}

	@TLFunctionAnnotation("Converts date to string according to the specified pattern.")
	public static final String date2str(Date date, String pattern) {
		final SimpleDateFormat format = new SimpleDateFormat();
		format.applyPattern(pattern);
		return format.format(date);
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

	// TODO: add test case
	@TLFunctionAnnotation("Narrowing conversion from long to integer value.")
	public static final Integer long2int(Long l) {
		return l.intValue();
	}
	
	class Long2IntFunction implements TLFunctionPrototype {
		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(long2int(stack.popLong()));
		}
	}
	
	// TODO: add test case
	@TLFunctionAnnotation("Narrowing conversion from double to integer value.")
	public static final Integer double2int(Double l) {
		return l.intValue();
	}
	class Double2IntFunction implements TLFunctionPrototype {
		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(double2int(stack.popDouble()));
		}
	}
	
	// TODO: add test case
	@TLFunctionAnnotation("Narrowing conversion from decimal to integer value.")
	public static final Integer decimal2int(BigDecimal l) {
		return l.intValue();
	}
	class Decimal2IntFunction implements TLFunctionPrototype {
		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(decimal2int(stack.popDecimal()));
		}
	}
	
	// TODO: add test case
	@TLFunctionAnnotation("Narrowing conversion from double to long value.")
	public static final Long double2long(Double d) {
		return d.longValue();
	}
	
	class Double2LongFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(double2long(stack.popDouble()));
		}
	}
	
	// TODO: add test case
	@TLFunctionAnnotation("Narrowing conversion from decimal to long value.")
	public static final Long decimal2long(BigDecimal d) {
		return d.longValue();
	}

	class Decimal2LongFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(decimal2long(stack.popDecimal()));
		}
	}

	// TODO: add test case
	@TLFunctionAnnotation("Narrowing conversion from decimal to double value.")
	public static final Double decimal2double(BigDecimal d) {
		return d.doubleValue();
	}

	class Decimal2DoubleFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(decimal2double(stack.popDecimal()));
		}
	}

	

	// NUM2BOOL
	@TLFunctionAnnotation("Converts 0 to false and any other numeric value to true.")
	public static final Boolean num2bool(int b) {
		return b != 0;
	}
	
	@TLFunctionAnnotation("Converts 0 to false and any other numeric value to true.")
	public static final Boolean num2bool(long b) {
		return b != 0;
	}
	
	@TLFunctionAnnotation("Converts 0 to false and any other numeric value to true.")
	public static final Boolean num2bool(double b) {
		return b != 0;
	}
	
	@TLFunctionAnnotation("Converts 0 to false and any other numeric value to true.")
	public static final Boolean num2bool(BigDecimal b) {
		return BigDecimal.ZERO.compareTo(b) != 0;
	}

	class Num2BoolFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[0].isInteger()) {
				stack.push(num2bool(stack.popInt()));
			} else if (actualParams[0].isLong()) {
				stack.push(num2bool(stack.popLong()));
			} else if (actualParams[0].isDouble()) {
				stack.push(num2bool(stack.popDouble()));
			} else if (actualParams[0].isDecimal()) {
				stack.push(num2bool(stack.popDecimal()));
			}
		}
		
	}

	
	@TLFunctionAnnotation("Converts true to 1 and false to 0.")
	public static final Integer bool2num(boolean b) {
		return b ? 1 : 0;
	}
	
	// BOOL2NUM
	class Bool2NumFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(bool2num(stack.popBoolean()));
		}

	}

	@TLFunctionAnnotation("Converts string to true if and onle if it is identical to string 'true'. False otherwise")
	public static final Boolean str2bool(String s) {
		return "true".equals(s);
	}
	
	// TODO: add test case
	// STR2BOOL
	class Str2BoolFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(str2bool(stack.popString()));
		}
	}

	
	// this method should is not annotated as it should not be directly visible in CTL
	private static final String to_string_internal(Object o) {
		return o != null ? o.toString() : "null";
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final String to_string(int i) {
		return to_string_internal(i);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final String to_string(long l) {
		return to_string_internal(l);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final String to_string(double d) {
		return to_string_internal(d);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final String to_string(BigDecimal d) {
		return to_string_internal(d);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final <E> String to_string(List<E> list) {
		return to_string_internal(list);
	}
	
	@TLFunctionAnnotation("Returns string representation of its argument")
	public static final <K,V> String to_string(Map<K,V> map) {
		return to_string_internal(map);
	}
	
	// TODO: add test case
	// to_string
	class ToStringFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(to_string_internal(stack.pop()));
		}

	}

	
	@TLFunctionAnnotation("Returns date from long that represents milliseconds from epoch")
	public static final Date long2date(Long l) {
		return new Date(l);
	}
	// TODO: add test case
	// Long2Date
	class Long2DateFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(long2date(stack.popLong()));
		}

	}

	
	@TLFunctionAnnotation("Returns long that represents milliseconds from epoch to a date")
	public static final Long date2long(Date d) {
		return d.getTime();
	}
	// TODO: add test case
	// DATE2LONG
	class Date2LongFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(date2long(stack.popDate()));
		}

	}
	
	@TLFunctionAnnotation("Converts binary data encoded in base64 to array of bytes.")
	public static final ByteArray base64byte(String src) {
		ByteArray array = new ByteArray();
		array.decodeBase64(src);
		return array;
	}	
	
	// BASE64BYTE
	public class Base64ByteFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(base64byte(stack.popString()));
		}
	}
	
	@TLFunctionAnnotation("Converts binary data into their base64 representation.")
	public static final String byte2base64(ByteArray src) {
		return src.encodeBase64();
	}
	
	// BYTE2BASE64
	public class Byte2Base64Function implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(byte2base64(stack.popByteArray()));
		}
	}
	
	@TLFunctionAnnotation("Converts bits into their string representation.")
	public static final String bits2str(ByteArray src) {
		int length = src.length();
		return src.decodeBitString('1', '0', 0, length == 0 ? 0 : (src.length() << 3) - 1).toString();
	}

	// BITS2STR
	public class Bits2StrFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(bits2str(stack.popByteArray()));
		}
	}
	
	@TLFunctionAnnotation("Converts string representation of bits into binary value.")
	public static final ByteArray str2bits(String src) {
		ByteArray array = new ByteArray();
		array.encodeBitString(src, '1', true);
		return array;
	}

	// STR2BITS
	public class Str2BitsFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(str2bits(stack.popString()));
		}
	}

	@TLFunctionAnnotation("Converts binary data into hex string.")
	public static final String byte2hex(ByteArray src) {
		StringBuilder strVal = new StringBuilder(src.length());
		for (int i = 0; i < src.length(); i++) {
			strVal.append(Character.forDigit((src.getByte(i) & 0xF0) >> 4, 16));
			strVal.append(Character.forDigit(src.getByte(i) & 0x0F, 16));
		}
		return strVal.toString();
	}
	
	// BYTE2HEX
	public class Byte2HexFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(byte2hex(stack.popByteArray()));
		}
	}
	
	@TLFunctionAnnotation("Converts hex string into binary.")
	public static final ByteArray hex2byte(String src) {
		ByteArray array = new ByteArray();
		char[] charArray = src.toCharArray();
		for (int i = 0; i < charArray.length - 1; i = i + 2) {
			array.append((byte) (((byte) Character.digit(charArray[i], 16) << 4) | (byte) Character.digit(charArray[i + 1], 16)));
		}
    	return array;
	}

	// HEX2BYTE
	public class Hex2ByteFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(hex2byte(stack.popString()));
		}
	}
	
	@TLFunctionAnnotation("Converts long into packed decimal representation (bytes).")
	public static final ByteArray long2packdecimal(Long src) {
		ByteArray bytes = new ByteArray(16);
		byte[] tmp = new byte[16];
		int length = PackedDecimal.format(src, tmp);
		bytes.setValue(tmp);
		bytes.setLength(length);
		return bytes;
	}
	
	// LONG2PACKEDDECIMAL
	class Long2PackedDecimalFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(long2packdecimal(stack.popLong()));
		}
	}
	
	@TLFunctionAnnotation("Converts packed decimal(bytes) into long value.")
	public static final Long packdecimal2long(ByteArray array) {
		return PackedDecimal.parse(array.getValue());
	}
	
	// PACKEDDECIMAL2LONG
	class PackedDecimal2LongFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(packdecimal2long(stack.popByteArray()));
		}
	}

	@TLFunctionAnnotation("Calculates MD5 hash of input string.")
	public static final ByteArray md5(String src) {
		return new ByteArray(Digest.digest(DigestType.MD5, src));
	}
	
	@TLFunctionAnnotation("Calculates MD5 hash of input bytes.")
	public static final ByteArray md5(ByteArray src) {
		return new ByteArray(Digest.digest(DigestType.MD5, src.getValue()));
	}
	
	// MD5
	class MD5Function implements TLFunctionPrototype {
	
		public void execute(Stack stack, TLType[] actualParams) {
			if(actualParams[0].isString()) {
				stack.push(md5(stack.popString()));
			} else {
				stack.push(md5(stack.popByteArray()));
			}
		}
	}
	
	@TLFunctionAnnotation("Calculates SHA hash of input bytes.")
	public static final ByteArray sha(ByteArray src) {
		return new ByteArray(Digest.digest(DigestType.SHA, src.getValue()));
	}
	
	@TLFunctionAnnotation("Calculates SHA hash of input string.")
	public static final ByteArray sha(String src) {
		return new ByteArray(Digest.digest(DigestType.SHA, src));
	}
	
	// SHA
	class SHAFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if(actualParams[0].isString()) {
				stack.push(sha(stack.popString()));
			} else {
				stack.push(sha(stack.popByteArray()));
			}
		}
	}

	@TLFunctionAnnotation("Returns name of i-th field of passed-in record.")
	public static final String get_field_name(DataRecord record, Integer position) {
		return record.getField(position).getMetadata().getName();
	}
	
	//GET_FIELD_NAME
	class GetFieldNameFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			Integer position = stack.popInt();
			DataRecord record = stack.popRecord();
			stack.push(get_field_name(record, position));
		}
	}
	
	@TLFunctionAnnotation("Returns data type of i-th field of passed-in record")
	public static final String get_field_type(DataRecord record, Integer position) {
		return record.getField(position).getMetadata().getTypeAsString();
	}

	//GET_FIELD_TYPE
	class GetFieldTypeFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			Integer position = stack.popInt();
			DataRecord record = stack.popRecord();
			stack.push(get_field_type(record, position));
		}
	}
}
