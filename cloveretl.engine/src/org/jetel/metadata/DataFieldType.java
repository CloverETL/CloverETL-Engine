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

	STRING("string", (byte) 0, CloverString.class, false, false, 'S') {
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

	DATE("date", (byte) 1, Date.class, false, true, 'D') {
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

	NUMBER("number", (byte) 2, Double.class, true, true,'N') {
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

	INTEGER("integer", (byte) 3, Integer.class, true, true, 'i') {
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

	LONG("long", (byte) 4, Long.class, true, true, 'l') {
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

	DECIMAL("decimal", (byte) 5, Decimal.class, true, true, 'd') {
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

	BYTE("byte", (byte) 6, byte[].class, false, false, 'B') {
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

	CBYTE("cbyte", (byte) 7, byte[].class, false, false, 'Z') {
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
	
	BOOLEAN("boolean", (byte) 8, Boolean.class, false, true, 'b') {
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

	NULL("null", (byte) 100, Void.class, false, false, 'n'),

	UNKNOWN("unknown", (byte) 101, Void.class, false, false, ' '),

	@Deprecated
	SEQUENCE("sequence", (byte) 102, Void.class, false, false, 'q'),

	/**
	 * @deprecated use {@link #DATE} instead
	 */
	@Deprecated
	DATETIME("datetime", (byte) 103, Date.class, false, true, 'T') {
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
	//short data type identification (it is identical with old fashion character identification of a type)
	private char shortName;
	
	/** This byte identifier is used by metadata serialisation into a byte stream.
	 * (This is used by CloverDataReader/Writer to serialise used metadata into data file.)
	 * DataFieldType.ordinal() could be used instead, but this could be non-intentionally
	 * changed by adding new data type. So custom ordinal number is used instead to ensure
	 * stability against code changes. */
	private byte byteIdentifier;
	
	private DataFieldType(String name, byte byteIdentifier,  Class<?> clazz, boolean isNumeric, boolean isTrimType, char shortName) {
		this.name = name;
		this.byteIdentifier = byteIdentifier;
		this.clazz = clazz;
		this.isNumeric = isNumeric;
		this.isTrimType = isTrimType;
		this.shortName = shortName;
	}

	/**
	 * @return name of type
	 */
	public String getName() {
		return name;
	}
	
	/** This byte identifier is used by metadata serialisation into a byte stream.
	 * (This is used by CloverDataReader/Writer to serialise used metadata into data file.)
	 * DataFieldType.ordinal() could be used instead, but this could be non-intentionally
	 * changed by adding new data type. So custom ordinal number is used instead to ensure
	 * stability against code changes. */
	public byte getByteIdentifier() {
		return byteIdentifier;
	}
	
	@Override
	public String toString() {
		return name;
	}
	
	/**
	 * Returns string form of this data field type in relation with a container type.
	 * For example integer data type with
	 * <li>SINGLE container returns 'integer'
	 * <li>LIST container returns 'integer[]'
	 * <li>MAP container returns 'map[string, integer]'
	 * @param containerType
	 * @return
	 */
	public String toString(DataFieldContainerType containerType) {
		if (containerType == null) {
			return toString();
		} else {
			switch(containerType) {
			case SINGLE:
				return toString();
			case LIST:
				return toString() + "[]";
			case MAP:
				return "map[string, " + toString() + "]";
			default:
				throw new IllegalArgumentException("unsupported container type");
			}
		}
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
	 * @return obsolete character identification
	 * @deprecated use {@link #getShortName()} instead
	 */
	@Deprecated
	public char getObsoleteIdentifier() {
		return getShortName();
	}

	/**
	 * @return short data type identification
	 */
	public char getShortName() {
		return shortName;
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
	
	/**
	 * @param byteIdentifier
	 * @return data type associated with given byte identifier
	 * @see #getByteIdentifier()
	 */
	public static DataFieldType fromByteIdentifier(byte byteIdentifier) {
		for (DataFieldType dataType : values()) {
			if (dataType.getByteIdentifier() == byteIdentifier) {
				return dataType;
			}
		}
		
		throw new IllegalArgumentException("Unknown data type identifier '" + byteIdentifier + "'.");
	}
	
}
