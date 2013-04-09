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
package org.jetel.ctl.data;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jetel.ctl.TLUtils;
import org.jetel.ctl.TransformLangParserConstants;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.EqualsUtil;

public abstract class TLType {

	public static final TLTypeVoid VOID = new TLTypeVoid();
	public static final TLTypeNull NULL = new TLTypeNull();
	public static final TLTypeError ERROR = new TLTypeError();
	public static final TLTypeByteArray BYTEARRAY = new TLTypeByteArray();
	public static final TLTypeRecord RECORD = new TLTypeRecord(null);
	
	
	// special type that can hold any value-type, used only for CTL internal functions and lookup params
	public static final TLTypeObject OBJECT = new TLTypeObject();

	/**
	 * Human-readable name for this type. Used for error reporting and messages
	 * as opposed to toString().
	 * @return
	 */
	public abstract String name();
	
	/**
	 * Returns 'least common multiple' for this and other types.
	 * @param otherType
	 * @return
	 */
	public abstract TLType promoteWith(TLType otherType);
	public abstract boolean isNumeric();
	
	@Override
	public String toString() {
		return name();
	}
	
	/**
	 * Tests assignment compatibility of this type with the other type.
	 * @param otherType
	 * @return
	 */
	public boolean canAssign(TLType otherType) {
		return otherType == TLType.NULL || this.promoteWith(otherType).equals(this);
	}

	
	
	public static final class TLTypeObject extends TLType {

		TLTypeObject() {
			// do not instantiate me, use constant above
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType == TLType.ERROR) {
				return otherType;
			}
			
			if (otherType == TLType.VOID) {
				return TLType.ERROR;
			}
			
			return this;
		}

		@Override
		public boolean isNumeric() {
			return false;
		}

		@Override
		public String name() {
			return "object";
		}
		
	}
	
	public static final class TLTypeVoid extends TLType {
		TLTypeVoid() {
			// do not instantiate me, use constant above
		}
		
		@Override
		public String name() {
			return "void";
		}
		
		@Override
		public boolean isNumeric() {
			return false;
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType == TLType.VOID) {
				return TLType.VOID;
			}
		
			return TLType.ERROR;
		}
		
	}
	
	public static final class TLTypeNull extends TLType {
		TLTypeNull() {
			// do not instantiate me, use constant
		}
		
		@Override
		public String name() {
			return "null";
		}
		
		@Override
		public boolean isNumeric() {
			return false;
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			return otherType;
		}
	}
	
	
	public static final class TLTypeError extends TLType {
		private TLTypeError() {
			// do not instantiate me, use constant
		}
		
		@Override
		public String name() {
			return "error";
		}
		
		@Override
		public boolean isNumeric() {
			return false;
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			return this;
		}
	}
	
	public static final class TLTypeList extends TLType {
		private TLType elementType;

		private TLTypeList(TLType elementType) {
			this.elementType = elementType;
		}
		
		public TLType getElementType() {
			return elementType;
		}
		
		@Override
		public String name() {
			return (elementType ==null ? "?" : elementType.toString() ) + "[]";
		}
		
		
		@Override
		public boolean isNumeric() {
			return false;
		}
		
		@Override
		public boolean isParameterized() {
			return elementType.isParameterized();
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType.isList()) {
				if (this.equals(otherType)) {
					return this;
				}
			}

			return TLType.ERROR;
		}

		@Override
		public int hashCode() {
			final int prime = 37;
			int result = 1;
			result = prime * result + ((elementType == null) ? 0 : elementType.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof TLTypeList))
				return false;
			final TLTypeList other = (TLTypeList) obj;
			if (elementType == null) {
				if (other.elementType != null)
					return false;
			} else if (!elementType.equals(other.elementType))
				return false;
			return true;
		}

	}
	
	
	public static TLTypeList createList(TLType elemType) {
		return new TLTypeList(elemType);
	}
	
	public static final class TLTypeRecord extends TLType {
		private final DataRecordMetadata metadata;
		
		private TLTypeRecord(DataRecordMetadata metadata) {
			this.metadata = metadata;
		}
		
		@Override
		public String name() {
			String ret = "record";
			return metadata != null ?  ret + "(" + metadata.getName() + ")" : ret;  
		}
		
		public DataRecordMetadata getMetadata() {
			return metadata;
		}
		
		@Override
		public boolean isNumeric() {
			return false;
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			return equals(otherType) ? this : TLType.ERROR;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((metadata == null) ? 0 : metadata.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			
			if (obj == null) {
				return false;
			}
			
			if (!(obj instanceof TLTypeRecord)) {
				return false;
			}
			
			final TLTypeRecord other = (TLTypeRecord) obj;
			
			if (metadata == null) {
				if (other.metadata != null) {
					return false;
				}
			} else if (!TLUtils.equals(metadata, other.metadata)) {
				return false;
			}
			
			return true;
		}
	}
	
	public static final class TLTypeMap extends TLType {
		private TLType keyType;
	 	private TLType valueType;

		private TLTypeMap(TLType key, TLType value) {
	 		this.keyType = key;
	 		this.valueType = value;
	 	}
	 	
		public TLType getKeyType() {
			return keyType;
		}
		
		public TLType getValueType() {
			return valueType;
		}
		
	 	@Override
	 	public String name() {
	 		/*System.out.println("zzzzzzzzzzzzzzzzzz");
	 		System.out.println("map["); 
	 		System.out.println(keyType == null ? "?" : keyType.name()); 
			 System.out.println(",");
			 System.out.println(valueType == null ? "?" : valueType.name());
			 System.out.println("]");
			 System.out.println("map[" + 
			 		(keyType == null ? "?" : keyType.name()) 
			 		+ "," + (valueType == null ? "?" : valueType.name()) 
			 		+ "]");*/
	 		return "map[" + 
			 		(keyType == null ? "?" : keyType.name()) 
			 		+ "," + (valueType == null ? "?" : valueType.name()) 
			 		+ "]";
	 	}
	 	
	 	
	 	@Override
	 	public boolean isNumeric() {
	 		return false;
	 	}
	 	
	 	@Override
	 	public boolean isParameterized() {
	 		return keyType.isParameterized() || valueType.isParameterized();
	 	}
	 	
	 	@Override
	 	public TLType promoteWith(TLType otherType) {
	 		return this.equals(otherType) ? this : TLType.ERROR;
	 	}

		@Override
		public int hashCode() {
			final int prime = 43;
			int result = 1;
			result = prime * result + ((keyType == null) ? 0 : keyType.hashCode());
			result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof TLTypeMap))
				return false;
			final TLTypeMap other = (TLTypeMap) obj;
			if (keyType == null) {
				if (other.keyType != null)
					return false;
			} else if (!keyType.equals(other.keyType))
				return false;
			if (valueType == null) {
				if (other.valueType != null)
					return false;
			} else if (!valueType.equals(other.valueType))
				return false;
			return true;
		}
	 	
	 	
	}
	
	public static TLTypeMap createMap(TLType key, TLType value) {
		return new TLTypeMap(key,value);
	}
	
	public static final TLTypeRecord forRecord(DataRecordMetadata meta) {
		return new TLTypeRecord(meta);
	}
	
	
	public static final class TLTypeByteArray extends TLType {
		private TLTypeByteArray() {

		}

		@Override
		public String name() {
			return "byte";
		}

		@Override
		public boolean isNumeric() {
			return false;
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType == TLType.BYTEARRAY) {
				return this;
			}
			
			if (otherType.isNull()) {
				return this;
			}
			
			return TLType.ERROR;
		}
	}
	
	public abstract static class TLTypeSymbol extends TLType {
		
		@Override
		public boolean isNumeric() {
			return false;
		}
		
		public abstract boolean isLogLevel();		
		public abstract boolean isDateField();
		public abstract Object getSymbol();
		
		@Override
		public TLType promoteWith(TLType otherType) {
			return TLType.ERROR;
		}
		
	}
	
	public static final class TLDateField extends TLTypeSymbol {
		private final DateFieldEnum symbol;
		
		public TLDateField(DateFieldEnum symbol) {
			this.symbol = symbol;
		}
		
		@Override
		public String name() {
			return "timeunit";
		}
		
		@Override
		public Object getSymbol() {
			return symbol;
		}
		
		@Override
		public boolean isLogLevel() {
			return false;
		}
		
		@Override
		public boolean isDateField() {
			return true;
		}
		
		@Override
		public boolean canAssign(TLType otherType) {
			return otherType instanceof TLDateField;
		}

		@Override
		public int hashCode() {
			final int prime = 1039;
			int result = 1;
			result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof TLDateField))
				return false;
			final TLDateField other = (TLDateField) obj;
			if (symbol == null) {
				if (other.symbol != null)
					return false;
			} else if (!symbol.equals(other.symbol))
				return false;
			return true;
		}
		
	}

	public static final class TLLogLevel extends TLTypeSymbol {
		private LogLevelEnum symbol;
		
		public TLLogLevel(LogLevelEnum symbol ) {
			this.symbol = symbol;
		}
		
		@Override
		public Object getSymbol() {
			return symbol;
		}
		
		@Override
		public String name() {
			return "loglevel";
		}
		
		@Override
		public boolean isLogLevel() {
			return true;
		}
		
		@Override
		public boolean isDateField() {
			return false;
		}
		
		@Override
		public boolean canAssign(TLType otherType) {
			return otherType instanceof TLLogLevel;
		}

		@Override
		public int hashCode() {
			final int prime = 1031;
			int result = 1;
			result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof TLLogLevel)) {
				return false;
			}
			final TLLogLevel other = (TLLogLevel) obj;
			return EqualsUtil.areEqual(symbol, other.symbol);
		}

	}
	
	public static TLTypeSymbol createTypeSymbol(int symbol) {
		switch (symbol){
		case TransformLangParserConstants.DAY:
		case TransformLangParserConstants.MONTH:
		case TransformLangParserConstants.WEEK:
		case TransformLangParserConstants.YEAR:
		case TransformLangParserConstants.HOUR:
		case TransformLangParserConstants.MINUTE:
		case TransformLangParserConstants.SECOND:
		case TransformLangParserConstants.MILLISEC:
			return new TLDateField(DateFieldEnum.fromToken(symbol));
		case TransformLangParserConstants.LOGLEVEL_DEBUG:
		case TransformLangParserConstants.LOGLEVEL_ERROR:
		case TransformLangParserConstants.LOGLEVEL_FATAL:
		case TransformLangParserConstants.LOGLEVEL_INFO:
		case TransformLangParserConstants.LOGLEVEL_WARN:
		case TransformLangParserConstants.LOGLEVEL_TRACE:
			return new TLLogLevel(LogLevelEnum.fromToken(symbol));
		default:
			throw new IllegalArgumentException("Illegal type symbol (" + symbol + ")");
		}
	}
	
	/**
	 * Type representing Java type variables.
	 * Exists only in types describing external Java functions from CTL libraries.
	 * Is replaced in TypeChecker for actual types in function call.
	 */
	public static final class TLTypeVariable extends TLType {
		private final String name;

		public TLTypeVariable(String name) {
			this.name = name;
		}

		@Override
		public boolean isNumeric() {
			return false;
		}

		@Override
		public String name() {
			return name;
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			return TLType.ERROR;
		}
		
		@Override
		public boolean isParameterized() {
			return true;
		}
		
	}

	public static final TLTypeVariable typeVariable(String name) {
		return new TLTypeVariable(name);
	}
	
	public boolean isDate() {
		return this == TLTypePrimitive.DATETIME;
	}
	
	public boolean isInteger() {
		return this == TLTypePrimitive.INTEGER;
	}
	
	public boolean isLong() {
		return this == TLTypePrimitive.LONG;
	}
	
	public boolean isDouble() {
		return this == TLTypePrimitive.DOUBLE;
	}
	
	public boolean isDecimal() {
		return this == TLTypePrimitive.DECIMAL;
	}
	
	public boolean isBoolean() {
		return this == TLTypePrimitive.BOOLEAN;
	}
	
	public boolean isString() {
		return this == TLTypePrimitive.STRING;
	}
	
	public boolean isRecord() {
		return (this instanceof TLTypeRecord);
	}
	
	public boolean isByteArray() {
		return (this instanceof TLTypeByteArray);
	}
	
	public boolean isPrimitive() {
		return false;
	}
	
	public boolean isTypeSymbol() {
		return (this instanceof TLTypeSymbol);
	}
	
	public boolean isMap() {
		return (this instanceof TLTypeMap);
	}
	
	public boolean isList() {
		return (this instanceof TLTypeList);
	}
	
	public boolean isIterable() {
		return isList() || isMap() || isRecord();
	}
	
	public boolean isValueType() {
		return this != TLType.VOID && this != TLType.ERROR && !isTypeSymbol();
	}
	
	public boolean isError() {
		return (this instanceof TLTypeError);
	}
	
	public boolean isVoid() {
		return this == TLType.VOID;
	}
	
	public boolean isNull() {
		return this ==TLType.NULL;
	}
	
	public boolean isObject() {
		return this == TLType.OBJECT;
	}
	
	public boolean isTypeVariable() {
		return this instanceof TLTypeVariable;
	}
	
	/**
	 * @return true when type is or contains a TypeVariable as its element type
	 */
	public boolean isParameterized() {
		return false;
	}
	
	public static TLType fromJavaType(Class<?> type) {
	
		if (byte.class.equals(type) || Byte.class.equals(type)) {
			return TLTypePrimitive.BYTEARRAY;
		}
		
		if (int.class.equals(type) || Integer.class.equals(type)) {
			return TLTypePrimitive.INTEGER;
		}
		
		if (long.class.equals(type) || Long.class.equals(type)) {
			return TLTypePrimitive.LONG;
		}
		
		if (double.class.equals(type) || Double.class.equals(type)) {
			return TLTypePrimitive.DOUBLE;
		}
		
		if (BigDecimal.class.equals(type)) {
			return TLTypePrimitive.DECIMAL;
		}
		
		
		if (boolean.class.equals(type) || Boolean.class.equals(type)) {
			return TLTypePrimitive.BOOLEAN;
		}
		
		if (Date.class.equals(type)) {
			return TLTypePrimitive.DATETIME;
		}
		
		if (String.class.equals(type)) {
			return TLTypePrimitive.STRING;
		}

		if (List.class.equals(type)) {
			throw new IllegalArgumentException("List cannot be created by fromJavaType(Class)");
		}
		
		if (Map.class.equals(type)) {
			throw new IllegalArgumentException("Map cannot be created by fromJavaType(Class)");
		}
		
		if (void.class.equals(type)) {
			return TLTypePrimitive.VOID;
		}
		
		if (DataRecord.class.equals(type)) {
			return TLType.RECORD;
		}
		
		throw new IllegalArgumentException("Type is not representable in CTL: " + type.getName() );
	}

	
	public static final TLType fromJavaType(Type toConvert) {
		if (toConvert instanceof TypeVariable) {
			// type variable
			return TLType.typeVariable(((TypeVariable<?>)toConvert).getName());
		} else if (toConvert instanceof ParameterizedType) {
			// parameterized: list or map
			ParameterizedType paramType = (ParameterizedType)toConvert;
			final Type[] params = paramType.getActualTypeArguments();
			Class<?> rawType = (Class<?>)paramType.getRawType();
			if (List.class.equals(rawType)) {
    			return TLType.createList(fromJavaType(params[0]));
    		} else if (Map.class.equals(rawType)) {
    			// extract key and value types from parameterized map
    			// map must be parameterized. if not, the method should be declared with type variables
    			return TLType.createMap(fromJavaType(params[0]),fromJavaType(params[1]));
    		} else {
    			throw new IllegalArgumentException("Unsupported parameterized type: " + rawType.getName());
    		}
		} else if (toConvert instanceof GenericArrayType) {
			return TLType.fromJavaType(((GenericArrayType)toConvert).getGenericComponentType());
		}else if (toConvert instanceof Class) {
			// non-generic parameter (possibly parameterized)
			Class<?> rawType = (Class<?>)toConvert;
			if (rawType.isArray()) {
				// we will receive this for var-arg functions
				// so we will convert it into a single paramter of array-element type
				return TLType.fromJavaType(rawType.getComponentType());
			} else if (rawType.isEnum()) {
				// we will receive this for functions accepting type symbols (date fields, log levels, etc)
				if (DateFieldEnum.class.equals(rawType)) {
					// function accepts date field symbols
					return TLType.createTypeSymbol(TransformLangParserConstants.YEAR);
				} else if (LogLevelEnum.class.equals(rawType)) {
					return TLType.createTypeSymbol(TransformLangParserConstants.LOGLEVEL_INFO);
				}
			} else if (DataRecord.class.equals(rawType)) {
				return TLType.RECORD;
			} else {
    			return TLType.fromJavaType(rawType);
    		}
		}
		
		throw new IllegalArgumentException("Unsupported type to convert: '" + toConvert);
	}
	
	public static TLType[] fromJavaType(Class<?>[] types) {
		final TLType[] ret = new TLType[types.length];
		for (int i=0; i<types.length; i++) {
			ret[i] = fromJavaType(types[i]);
		}
		
		return ret;
	}

	public static TLType[] fromJavaObjects(Object[] objects) {
		final TLType[] ret = new TLType[objects.length];
		for (int i = 0; i < objects.length; i++) {
			ret[i] = fromJavaType(objects[i].getClass());
		}
		
		return ret;
	}
	
	public static int distance(TLType from, TLType to) {
		if (from.isInteger()) {
			return	to.isInteger() ?	0	:
					to.isLong() 	?	1		:
					to.isDouble()	?	2		:
					to.isDecimal()	?	3		:	
					to.isTypeVariable()	?	10		:	Integer.MAX_VALUE;
		}

		if (from.isLong()) {
			return	to.isLong() 	?	0		:
					to.isDouble()	?	1		:
					to.isDecimal()	?	2		:	
					to.isTypeVariable()	?	10		:	Integer.MAX_VALUE;
		}

		if (from.isDouble()) {
			return	to.isDouble()	?	0		:
					to.isDecimal()	?	1		:	
					to.isTypeVariable()	?	10		:	Integer.MAX_VALUE;
		}
		
		if (from.isDecimal()) {
			return	to.isDecimal()	?	0		:	
					to.isTypeVariable()	?	10		:	Integer.MAX_VALUE;
		}
		
		if (from.isString()) {
			return	to.isString() 	?	0		:
					to.isTypeVariable()	?	10		:	Integer.MAX_VALUE;
		}
		
		if (from.isBoolean()) {
			return	to.isBoolean() 	?	0		:
					to.isTypeVariable()	?	10		:	Integer.MAX_VALUE;
		}
		
		if (from.isDate()) {
			return	to.isDate() 	?	0		:
					to.isTypeVariable()	?	10		:	Integer.MAX_VALUE;
		}
		
		if (from.isRecord()) {
			if (to.isRecord()) {
				// this handles built-in functions where arguments have metadata set to null
				// as they work with *any* record
				TLTypeRecord toRecord = (TLTypeRecord)to;
				if (toRecord.getMetadata() == null) {
					return 1;
				} else {
					return from.equals(to) ? 0 : Integer.MAX_VALUE;
				}
			} else if (to.isTypeVariable()) {
				return 10;
			}

			return Integer.MAX_VALUE;
		}

		if (from.isList()) {
			if (to.isList()) {
				// this handles built-in functions handling generic lists (with any element type)
				TLTypeList toList = (TLTypeList)to;
				if (toList.getElementType().isTypeVariable()) {
					return 10;
				} else {
					return from.equals(to) ? 0 : Integer.MAX_VALUE;
				}
			} 

			return Integer.MAX_VALUE;
		}
		
		if (from.isMap()) {
			if (to.isMap()) {
				// this handles built-in functions handling generic maps (having any element type)
				TLTypeMap toMap = (TLTypeMap)to;
				if (toMap.getKeyType().isTypeVariable() && toMap.getValueType().isTypeVariable()) {
					return 10;
				} else {
					return from.equals(to) ? 0 : Integer.MAX_VALUE;
				}
			} 

			return Integer.MAX_VALUE;
		}

		if (from.isNull()) {
			if (to.isVoid()) {
				// should not happen
				return Integer.MAX_VALUE;
			}
			return 0;
		}
		
		if (from.isVoid()) {
			return Integer.MAX_VALUE;
		}
		
		if (from.isTypeSymbol()) {
			if (from.canAssign(to)) {
				return 0;
			}
			
			return Integer.MAX_VALUE;
		}
		
		if (from.isByteArray()) {
			return	to.isByteArray() 	?	0		:
					to.isTypeVariable()	?	10		:	Integer.MAX_VALUE;
		}
		
		throw new IllegalArgumentException(" Unknown types for type-distance calculation: '" 
				+ from.name() +  "' and '" + to.name() + "'");

	}

}

