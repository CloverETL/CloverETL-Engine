package org.jetel.ctl.data;

import org.jetel.metadata.DataFieldMetadata;

public abstract class TLTypePrimitive extends TLType {

	public static final TLTypeInteger INTEGER = new TLTypeInteger();
	public static final TLTypeLong LONG = new TLTypeLong();
	public static final TLTypeString STRING = new TLTypeString();
	public static final TLTypeBoolean BOOLEAN = new TLTypeBoolean();
	public static final TLTypeDateTime DATETIME = new TLTypeDateTime();
	public static final TLTypeDouble DOUBLE = new TLTypeDouble();
	public static final TLTypeDecimal DECIMAL = new TLTypeDecimal();
	
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

		public TLType promoteWith(TLType otherType) {
			if (otherType.isNumeric()) {
				return otherType;
			}

			if (otherType.isObject()) {
				return otherType;
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
			return "double";
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
		switch (field.getType()) {
		case DataFieldMetadata.INTEGER_FIELD:
			return INTEGER;
		case DataFieldMetadata.LONG_FIELD:
			return LONG;
		case DataFieldMetadata.NUMERIC_FIELD:
			return DOUBLE;
		case DataFieldMetadata.DECIMAL_FIELD:
			return DECIMAL;
		case DataFieldMetadata.DATE_FIELD:
			return DATETIME;
		case DataFieldMetadata.BYTE_FIELD:
			return TLType.BYTEARRAY;
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			return TLType.CBYTEARRAY;
		case DataFieldMetadata.STRING_FIELD:
			return STRING;
		case DataFieldMetadata.BOOLEAN_FIELD:
			return BOOLEAN;
		default:
			throw new UnknownTypeException(field.getTypeAsString());
		}
	}
	
	public static char toCloverType(TLType type) {
		if (type.isInteger()) {
			return DataFieldMetadata.INTEGER_FIELD;
		} else if (type.isLong()) {
			return DataFieldMetadata.LONG_FIELD;
		} else if (type.isDouble()) {
			return DataFieldMetadata.NUMERIC_FIELD;
		} else if (type.isDecimal()) {
			return DataFieldMetadata.DECIMAL_FIELD;
		} else if (type.isDate()) {
			return DataFieldMetadata.DATE_FIELD;
		} else if (type.isString()) {
			return DataFieldMetadata.STRING_FIELD;
		} else if (type.isBoolean()) {
			return DataFieldMetadata.BOOLEAN_FIELD;
		}
		
		throw new UnknownTypeException(type.name());
	}

	public boolean isPrimitive() {
		return true;
	}
	
}
