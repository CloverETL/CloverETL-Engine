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

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 16 Jan 2012
 */
public enum DataFieldType {

	LIST("list", false, false, 'L'),
	
	STRING("string", false, false, 'S') {
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

	DATE("date", false, true, 'D') {
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

	NUMBER("number", true, true,'N') {
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

	INTEGER("integer", true, true, 'i') {
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

	LONG("long", true, true, 'l') {
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

	DECIMAL("decimal", true, true, 'd') {
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

	BYTE("byte", false, false, 'B') {
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

	CBYTE("cbyte", false, false, 'Z') {
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
	
	BOOLEAN("boolean", false, true, 'b') {
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

	NULL("null", false, false, 'n'),

	UNKNOWN("unknown", false, false, ' '),

	@Deprecated
	SEQUENCE("sequence", false, false, 'q'),

	/**
	 * @deprecated use {@link #DATE} instead
	 */
	@Deprecated
	DATETIME("datetime", false, true, 'T') {
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

	private String name;
	private boolean isNumeric;
	//type should trimmed by default
	private boolean isTrimType;
	private char obsoleteIdentifier;
	
	/**
	 * 
	 */
	private DataFieldType(String name, boolean isNumeric, boolean isTrimType, char obsoleteIdentifier) {
		this.name = name;
		this.isNumeric = isNumeric;
		this.isTrimType = isTrimType;
		this.obsoleteIdentifier = obsoleteIdentifier;
	}

	public String getName() {
		return name;
	}
	
	public boolean isNumeric() {
		return isNumeric;
	}
	
	public boolean isTrimType() {
		return isTrimType;
	}
	
	public boolean isSubtype(DataFieldType otherType) {
		return false;
	}
	
	public char getObsoleteIdentifier() {
		return obsoleteIdentifier;
	}

	public static DataFieldType fromChar(char charIdentifier) {
		for (DataFieldType dataType : values()) {
			if (dataType.getObsoleteIdentifier() == charIdentifier) {
				return dataType;
			}
		}
		
		throw new IllegalArgumentException("Unknown data type '" + charIdentifier + "'.");
	}
	
	@SuppressWarnings("deprecation")
	public static DataFieldType fromName(String name) {
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
