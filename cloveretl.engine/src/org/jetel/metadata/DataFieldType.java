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
package org.jetel.metadata;

import java.util.Date;
import java.util.List;

import org.jetel.data.DataField;
import org.jetel.data.StringDataField;
import org.jetel.data.primitive.Decimal;
import org.jetel.util.string.CloverString;

/**
 * Enumeration of all build-in clover data types.
 * STRING, DATE, NUMBER, INTEGER, LONG, DECIMAL, BYTE, CBYTE, BOOLEAN, LIST and MAP
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 16 Jan 2012
 */
public enum DataFieldType {

	LIST("list", List.class, false, false, 'L'),
	
	STRING("string", CloverString.class, false, false, 'S') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case BYTE:
			case CBYTE:
			case STRING:
				return true;
			default:
				return false;
			}
		}
	},

	DATE("date", Date.class, false, true, 'D') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case BYTE:
			case CBYTE:
			case STRING:
			case DATE:
			case DATETIME:
				return true;
			default:
				return false;
			}
		}
	},

	NUMBER("number", Double.class, true, true,'N') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case BYTE:
			case CBYTE:
			case STRING:
			case NUMBER:
			case DECIMAL:
				return true;
			default:
				return false;
			}
		}
	},

	INTEGER("integer", Integer.class, true, true, 'i') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case BYTE:
			case CBYTE:
			case STRING:
			case INTEGER:
			case LONG:
			case NUMBER:
			case DECIMAL:
				return true;
			default:
				return false;
			}
		}
	},

	LONG("long", Long.class, true, true, 'l') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case BYTE:
			case CBYTE:
			case STRING:
			case LONG:
			case NUMBER:
			case DECIMAL:
				return true;
			default:
				return false;
			}
		}
	},

	DECIMAL("decimal", Decimal.class, true, true, 'd') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case BYTE:
			case CBYTE:
			case STRING:
			case DECIMAL:
			case NUMBER:
			case INTEGER:
			case LONG:
				return true;
			default:
				return false;
			}
		}
	},

	BYTE("byte", byte[].class, false, false, 'B') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case BYTE:
			case CBYTE:
				return true;
			default:
				return false;
			}
		}
	},

	CBYTE("cbyte", byte[].class, false, false, 'Z') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case BYTE:
			case CBYTE:
				return true;
			default:
				return false;
			}
		}
	},
	
	BOOLEAN("boolean", Boolean.class, false, true, 'b') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case STRING:
			case BOOLEAN:
				return true;
			default:
				return false;
			}
		}
	},

	NULL("null", Void.class, false, false, 'n'),

	UNKNOWN("unknown", Void.class, false, false, ' '),

	@Deprecated
	SEQUENCE("sequence", Void.class, false, false, 'q'),

	/**
	 * @deprecated use {@link #DATE} instead
	 */
	@Deprecated
	DATETIME("datetime", Date.class, false, true, 'T') {
		@Override
		public boolean isSubtype(DataFieldType otherType) {
			switch (otherType) {
			case BYTE:
			case CBYTE:
			case STRING:
			case DATETIME:
				return true;
			default:
				return false;
			}
		}
	};

	//name of type
	private String name;
	
	//internal value class type
	//StringDataField vs. CloverString
	//IntegerDataField vs. Integer, ... 
	private Class<?> clazz;
	
	//does the type represent a numeric type?
	private boolean isNumeric;
	//should be trimmed by default
	private boolean isTrimType;
	//old fashion character identification of a type
	private char obsoleteIdentifier;
	
	private DataFieldType(String name, Class<?> clazz, boolean isNumeric, boolean isTrimType, char obsoleteIdentifier) {
		this.name = name;
		this.clazz = clazz;
		this.isNumeric = isNumeric;
		this.isTrimType = isTrimType;
		this.obsoleteIdentifier = obsoleteIdentifier;
	}

	/**
	 * @return name of type
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * This class is type of internal representation of respective data field.
	 * For example type {@link #STRING} corresponds with {@link StringDataField}
	 * and its value is natively managed in a variable typed {@link CloverString}.
	 * This method returns just the class of the internal value representation of respective {@link DataField}.
	 * Moreover method {@link DataField#getValue()} returns an instance of this class. 
	 * @return class of internal value representation of this type (CloverString.class for STRING, Integer.class for INTEGER, ...)
	 */
	public Class<?> getInternalValueClass() {
		return clazz;
	}
	
	/**
	 * @return true if type is based on numbers
	 */
	public boolean isNumeric() {
		return isNumeric;
	}
	
	/**
	 * @return true if the type should be trimmed
	 */
	public boolean isTrimType() {
		return isTrimType;
	}
	
	/**
	 * @param otherType
	 * @return true if the given type is convertible to this type
	 */
	public boolean isSubtype(DataFieldType otherType) {
		return false;
	}
	
	/**
	 * @return obsolete characted identification
	 * @deprecated
	 */
	@Deprecated
	public char getObsoleteIdentifier() {
		return obsoleteIdentifier;
	}

	/**
	 * @param charIdentifier
	 * @return type based on obsolete character identification
	 * @deprecated
	 */
	@Deprecated
	public static DataFieldType fromChar(char charIdentifier) {
		for (DataFieldType dataType : values()) {
			if (dataType.getObsoleteIdentifier() == charIdentifier) {
				return dataType;
			}
		}
		
		throw new IllegalArgumentException("Unknown data type '" + charIdentifier + "'.");
	}
	
	/**
	 * @param name
	 * @return type based on name
	 */
	@SuppressWarnings("deprecation")
	public static DataFieldType fromName(String name) {
		if (name == null) {
			throw new IllegalArgumentException("type name is null");
		}
		
		//for backward compatibility
		if (name.equals(DataFieldMetadata.NUMERIC_TYPE_DEPRECATED)) {
			return NUMBER;
		}
		
		for (DataFieldType dataType : values()) {
			if (dataType.getName().equals(name)) {
				return dataType;
			}
		}
		
		throw new IllegalArgumentException("Unknown data type '" + name + "'.");
	}
	
}
