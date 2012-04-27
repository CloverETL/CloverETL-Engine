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

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;

public abstract class TLTypePrimitive extends TLType {

	public static final TLTypeInteger INTEGER = new TLTypeInteger();
	public static final TLTypeLong LONG = new TLTypeLong();
	public static final TLTypeString STRING = new TLTypeString();
	public static final TLTypeBoolean BOOLEAN = new TLTypeBoolean();
	public static final TLTypeDateTime DATETIME = new TLTypeDateTime();
	public static final TLTypeDouble DOUBLE = new TLTypeDouble();
	public static final TLTypeDecimal DECIMAL = new TLTypeDecimal();
	
	@Override
	public abstract TLType promoteWith(TLType otherType);

	
	public static final class TLTypeInteger extends TLTypePrimitive {
		private TLTypeInteger() {

		}

		@Override
		public String name() {
			return "integer";
		}

		@Override
		public boolean isNumeric() {
			return true;
		}

		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType.isNumeric()) {
				return otherType;
			}

			if (otherType.isObject()) {
				return otherType;
			}
			
			if (otherType.isNull()) {
				return this;
			}
			
			return TLType.ERROR;

		}
	}

	public static final class TLTypeString extends TLTypePrimitive {
		private TLTypeString() {

		}

		@Override
		public String name() {
			return "string";
		}

		@Override
		public boolean isNumeric() {
			return false;
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType.isObject()) {
				return otherType;
			}
			
			if (otherType.isString()) {
				return this;
			}
			
			if (otherType.isNull()) {
				return this;
			}
			
			return TLType.ERROR;
		}
	}

	public static final class TLTypeBoolean extends TLTypePrimitive {
		private TLTypeBoolean() {

		}

		@Override
		public String name() {
			return "boolean";
		}

		@Override
		public boolean isNumeric() {
			return false;
		}

		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType.isBoolean()) {
				return this;
			}
			
			if (otherType.isObject()) {
				return otherType;
			}
			
			if (otherType.isNull()) {
				return this;
			}

			return TLType.ERROR;
		}
	}

	public static final class TLTypeLong extends TLTypePrimitive {
		private TLTypeLong() {

		}

		@Override
		public String name() {
			return "long";
		}

		@Override
		public boolean isNumeric() {
			return true;
		}

		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType.isInteger()) {
				return this;
			}

			if (otherType.isNumeric()) {
				return otherType;
			}
			
			if (otherType.isObject()) {
				return otherType;
			}
			
			if (otherType.isNull()) {
				return this;
			}

			return TLType.ERROR;

		}
	}

	public static final class TLTypeDateTime extends TLTypePrimitive {
		private TLTypeDateTime() {

		}

		@Override
		public String name() {
			return "date";
		}

		@Override
		public boolean isNumeric() {
			return false;
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType.isDate()) {
				return this;
			}
			
			if (otherType.isObject()) {
				return otherType;
			}
			
			if (otherType.isNull()) {
				return this;
			}
			
			return TLType.ERROR;
		}
	}

	//int < long <  double < decimal
	public static final class TLTypeDouble extends TLTypePrimitive {
		private TLTypeDouble() {

		}

		@Override
		public String name() {
			return "number";
		}

		@Override
		public boolean isNumeric() {
			return true;
		}
		
		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType.isDecimal()) {
				return otherType;
			}
			
			if (otherType.isNumeric()) {
				return this;
			}
			
			if (otherType.isObject()) {
				return otherType;
			}
			
			if (otherType.isNull()) {
				return this;
			}
			
			return TLType.ERROR;
		}
	}
	
	public static final class TLTypeDecimal extends TLTypePrimitive {
		
		@Override
		public String name() {
			return "decimal";
		}
		
		@Override
		public boolean isNumeric() {
			return true;
		}
		
		
		@Override
		public TLType promoteWith(TLType otherType) {
			if (otherType.isNumeric()) {
				return this;
			}
			
			if (otherType.isObject()) {
				return otherType;
			}
			
			
			if (otherType.isNull()) {
				return this;
			}
			
			return TLType.ERROR;
		}
		
	}
	
	
	public static TLType fromCloverType(DataFieldMetadata field) throws UnknownTypeException {
		switch (field.getContainerType()) {
			case SINGLE:
				switch (field.getDataType()) {
				case INTEGER:
					return INTEGER;
				case LONG:
					return LONG;
				case NUMBER:
					return DOUBLE;
				case DECIMAL:
					return DECIMAL;
				case DATE:
					return DATETIME;
				case BYTE:
				case CBYTE:
					return BYTEARRAY;
				case STRING:
					return STRING;
				case BOOLEAN:
					return BOOLEAN;
				default:
					throw new UnknownTypeException(field.getDataType().toString());
				}
			case LIST:
				switch (field.getDataType()) {
				case INTEGER:
					return TLType.createList(INTEGER);
				case LONG:
					return TLType.createList(LONG);
				case NUMBER:
					return TLType.createList(DOUBLE);
				case DECIMAL:
					return TLType.createList(DECIMAL);
				case DATE:
					return TLType.createList(DATETIME);
				case BYTE:
				case CBYTE:
					return TLType.createList(BYTEARRAY);
				case STRING:
					return TLType.createList(STRING);
				case BOOLEAN:
					return TLType.createList(BOOLEAN);
				default:
					throw new UnknownTypeException(field.getDataType().toString());
				}
			case MAP:
				switch (field.getDataType()) {
				case INTEGER:
					return TLType.createMap(STRING, INTEGER);
				case LONG:
					return TLType.createMap(STRING, LONG);
				case NUMBER:
					return TLType.createMap(STRING, DOUBLE);
				case DECIMAL:
					return TLType.createMap(STRING, DECIMAL);
				case DATE:
					return TLType.createMap(STRING, DATETIME);
				case BYTE:
				case CBYTE:
					return TLType.createMap(STRING, BYTEARRAY);
				case STRING:
					return TLType.createMap(STRING, STRING);
				case BOOLEAN:
					return TLType.createMap(STRING, BOOLEAN);
				default:
					throw new UnknownTypeException(field.getDataType().toString());
				}
			default: throw new UnknownTypeException(field.getContainerType().toString());
		}
	}
	
	public static DataFieldType toCloverType(TLType type) {
		if (type.isInteger()) {
			return DataFieldType.INTEGER;
		} else if (type.isLong()) {
			return DataFieldType.LONG;
		} else if (type.isDouble()) {
			return DataFieldType.NUMBER;
		} else if (type.isDecimal()) {
			return DataFieldType.DECIMAL;
		} else if (type.isDate()) {
			return DataFieldType.DATE;
		} else if (type.isString()) {
			return DataFieldType.STRING;
		} else if (type.isBoolean()) {
			return DataFieldType.BOOLEAN;
		}
		
		throw new UnknownTypeException(type.name());
	}

	@Override
	public boolean isPrimitive() {
		return true;
	}
	
}
