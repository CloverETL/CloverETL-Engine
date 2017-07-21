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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;

import org.jetel.ctl.Stack;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.Decimal;
import org.jetel.metadata.DataFieldType;

public class DynamicLib extends TLFunctionLibrary {

	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		TLFunctionPrototype ret = 
       		"getBoolValue".equals(functionName) ? new GetValueFunction(Boolean.class) : 
       		"getByteValue".equals(functionName) ? new GetValueFunction(byte[].class) : 
       		"getDateValue".equals(functionName) ? new GetValueFunction(Date.class) : 
       		"getDecimalValue".equals(functionName) ? new GetValueFunction(BigDecimal.class) : 
       		"getIntValue".equals(functionName) ? new GetValueFunction(Integer.class) : 
       		"getLongValue".equals(functionName) ? new GetValueFunction(Long.class) : 
       		"getNumValue".equals(functionName) ? new GetValueFunction(Double.class) : 
       		"getStringValue".equals(functionName) ? new GetValueFunction(String.class) :
       		"setBoolValue".equals(functionName) ? new SetValueFunction<Boolean>() {

				@Override
				protected void setFieldValueIndex(TLFunctionCallContext context, DataRecord record, int fieldIndex,
						Boolean value) {
					setBoolValue(context, record, fieldIndex, value);
				}

				@Override
				protected void setFieldValueName(TLFunctionCallContext context, DataRecord record, String fieldName,
						Boolean value) {
					setBoolValue(context, record, fieldName, value);
				}
       			
       		} : 
       		"setByteValue".equals(functionName) ? new SetValueFunction<byte[]>() {

				@Override
				protected void setFieldValueIndex(TLFunctionCallContext context, DataRecord record, int fieldIndex,
						byte[] value) {
					setByteValue(context, record, fieldIndex, value);
				}

				@Override
				protected void setFieldValueName(TLFunctionCallContext context, DataRecord record, String fieldName,
						byte[] value) {
					setByteValue(context, record, fieldName, value);
				}
       			
       		} : 
       		"setDateValue".equals(functionName) ? new SetValueFunction<Date>() {

				@Override
				protected void setFieldValueIndex(TLFunctionCallContext context, DataRecord record, int fieldIndex,
						Date value) {
					setDateValue(context, record, fieldIndex, value);
				}

				@Override
				protected void setFieldValueName(TLFunctionCallContext context, DataRecord record, String fieldName,
						Date value) {
					setDateValue(context, record, fieldName, value);
				}
       			
       		} : 
       		"setDecimalValue".equals(functionName) ? new SetValueFunction<BigDecimal>() {

				@Override
				protected void setFieldValueIndex(TLFunctionCallContext context, DataRecord record, int fieldIndex,
						BigDecimal value) {
					setDecimalValue(context, record, fieldIndex, value);
				}

				@Override
				protected void setFieldValueName(TLFunctionCallContext context, DataRecord record, String fieldName,
						BigDecimal value) {
					setDecimalValue(context, record, fieldName, value);
				}
       			
       		} : 
       		"setIntValue".equals(functionName) ? new SetValueFunction<Integer>() {

				@Override
				protected void setFieldValueIndex(TLFunctionCallContext context, DataRecord record, int fieldIndex,
						Integer value) {
					setIntValue(context, record, fieldIndex, value);
				}

				@Override
				protected void setFieldValueName(TLFunctionCallContext context, DataRecord record, String fieldName,
						Integer value) {
					setIntValue(context, record, fieldName, value);
				}
       			
       		} : 
       		"setLongValue".equals(functionName) ? new SetValueFunction<Long>() {

				@Override
				protected void setFieldValueIndex(TLFunctionCallContext context, DataRecord record, int fieldIndex,
						Long value) {
					setLongValue(context, record, fieldIndex, value);
				}

				@Override
				protected void setFieldValueName(TLFunctionCallContext context, DataRecord record, String fieldName,
						Long value) {
					setLongValue(context, record, fieldName, value);
				}
       			
       		} : 
       		"setNumValue".equals(functionName) ? new SetValueFunction<Double>() {

				@Override
				protected void setFieldValueIndex(TLFunctionCallContext context, DataRecord record, int fieldIndex,
						Double value) {
					setNumValue(context, record, fieldIndex, value);
				}

				@Override
				protected void setFieldValueName(TLFunctionCallContext context, DataRecord record, String fieldName,
						Double value) {
					setNumValue(context, record, fieldName, value);
				}
       			
       		} : 
       		"setStringValue".equals(functionName) ? new SetValueFunction<String>() {

				@Override
				protected void setFieldValueIndex(TLFunctionCallContext context, DataRecord record, int fieldIndex,
						String value) {
					setStringValue(context, record, fieldIndex, value);
				}

				@Override
				protected void setFieldValueName(TLFunctionCallContext context, DataRecord record, String fieldName,
						String value) {
					setStringValue(context, record, fieldName, value);
				}
       			
       		} :
       		"isNull".equals(functionName) ? new IsNullFunction() :
       		"getValueAsString".equals(functionName) ? new GetValueAsStringFunction() :
       		"compare".equals(functionName) ? new CompareFunction() :
       		"getFieldIndex".equals(functionName) ? new GetFieldIndexFunction() :
           	"getFieldLabel".equals(functionName) ? new GetFieldLabelFunction() :
   			null; 
		
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
		
		return ret;
			
	}
	
	private static String LIBRARY_NAME = "Dynamic field access";

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}

		
	private static final Object getFieldValue(DataField field) {
		Object value = field.getValue();
		if (value == null) {
			return null;
		} else if(value instanceof CharSequence) {
			// convert to String
			value = ((CharSequence) value).toString();
		} else if(value instanceof Decimal) {
			// convert to BigDecimal
			value = ((Decimal) value).getBigDecimalOutput();
		} else if(value instanceof Date) {
			/* 
			 * create a new instance for safety
			 * - DateDataField can modify the returned instance 
			 */
			value = new Date(((Date) value).getTime());
		}
		return value;
	}
	
	private static final Object getFieldValue(DataRecord record, int fieldIndex) {
		return getFieldValue(record.getField(fieldIndex));
	}
	
	private static final Object getFieldValue(DataRecord record, String fieldName) {
		return getFieldValue(record.getField(fieldName));
	}
	
	// GET BOOL VALUE
	@TLFunctionAnnotation("Returns the boolean value of a field")
	public static final Boolean getBoolValue(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return (Boolean) getFieldValue(record, fieldIndex);
	}
	
	@TLFunctionAnnotation("Returns the boolean value of a field")
	public static final Boolean getBoolValue(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return (Boolean) getFieldValue(record, fieldName);
	}

	// GET BYTE VALUE
	@TLFunctionAnnotation("Returns the byte value of a field")
	public static final byte[] getByteValue(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return (byte[]) getFieldValue(record, fieldIndex);
	}
	
	@TLFunctionAnnotation("Returns the byte value of a field")
	public static final byte[] getByteValue(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return (byte[]) getFieldValue(record, fieldName);
	}

	// GET DATE VALUE
	@TLFunctionAnnotation("Returns the date value of a field")
	public static final Date getDateValue(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return (Date) getFieldValue(record, fieldIndex);
	}
	
	@TLFunctionAnnotation("Returns the date value of a field")
	public static final Date getDateValue(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return (Date) getFieldValue(record, fieldName);
	}

	// GET DECIMAL VALUE
	@TLFunctionAnnotation("Returns the decimal value of a field")
	public static final BigDecimal getDecimalValue(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return (BigDecimal) getFieldValue(record, fieldIndex);
	}
	
	@TLFunctionAnnotation("Returns the decimal value of a field")
	public static final BigDecimal getDecimalValue(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return (BigDecimal) getFieldValue(record, fieldName);
	}

	// GET INT VALUE
	@TLFunctionAnnotation("Returns the integer value of a field")
	public static final Integer getIntValue(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return (Integer) getFieldValue(record, fieldIndex);
	}
	
	@TLFunctionAnnotation("Returns the integer value of a field")
	public static final Integer getIntValue(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return (Integer) getFieldValue(record, fieldName);
	}

	// GET LONG VALUE
	@TLFunctionAnnotation("Returns the long value of a field")
	public static final Long getLongValue(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return (Long) getFieldValue(record, fieldIndex);
	}
	
	@TLFunctionAnnotation("Returns the long value of a field")
	public static final Long getLongValue(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return (Long) getFieldValue(record, fieldName);
	}

	// GET NUM VALUE
	@TLFunctionAnnotation("Returns the number value of a field")
	public static final Double getNumValue(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return (Double) getFieldValue(record, fieldIndex);
	}
	
	@TLFunctionAnnotation("Returns the number value of a field")
	public static final Double getNumValue(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return (Double) getFieldValue(record, fieldName);
	}

	// GET STRING VALUE
	@TLFunctionAnnotation("Returns the string value of a field")
	public static final String getStringValue(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return (String) getFieldValue(record, fieldIndex);
	}
	
	@TLFunctionAnnotation("Returns the string value of a field")
	public static final String getStringValue(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return (String) getFieldValue(record, fieldName);
	}
	

	private static final void setFieldValue(DataField field, Object value) {
		field.setValue(value);
	}
	
	// SET BOOL VALUE
	@TLFunctionAnnotation("Sets the boolean value of a field")
	public static final void setBoolValue(TLFunctionCallContext context, DataRecord record, int fieldIndex, Boolean value) {
		DataField field = record.getField(fieldIndex);
		if (field.getMetadata().getDataType() != DataFieldType.BOOLEAN) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}
	
	@TLFunctionAnnotation("Sets the boolean value of a field")
	public static final void setBoolValue(TLFunctionCallContext context, DataRecord record, String fieldName, Boolean value) {
		DataField field = record.getField(fieldName);
		if (field.getMetadata().getDataType() != DataFieldType.BOOLEAN) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}

	// SET BYTE VALUE
	@TLFunctionAnnotation("Sets the byte value of a field")
	public static final void setByteValue(TLFunctionCallContext context, DataRecord record, int fieldIndex, byte[] value) {
		DataField field = record.getField(fieldIndex);
		if ((field.getMetadata().getDataType() != DataFieldType.BYTE)
				&& (field.getMetadata().getDataType() != DataFieldType.CBYTE)) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}
	
	@TLFunctionAnnotation("Sets the byte value of a field")
	public static final void setByteValue(TLFunctionCallContext context, DataRecord record, String fieldName, byte[] value) {
		DataField field = record.getField(fieldName);
		if ((field.getMetadata().getDataType() != DataFieldType.BYTE)
				&& (field.getMetadata().getDataType() != DataFieldType.CBYTE)) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}

	// SET DATE VALUE
	@TLFunctionAnnotation("Sets the date value of a field")
	public static final void setDateValue(TLFunctionCallContext context, DataRecord record, int fieldIndex, Date value) {
		DataField field = record.getField(fieldIndex);
		if (field.getMetadata().getDataType() != DataFieldType.DATE) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}
	
	@TLFunctionAnnotation("Sets the date value of a field")
	public static final void setDateValue(TLFunctionCallContext context, DataRecord record, String fieldName, Date value) {
		DataField field = record.getField(fieldName);
		if (field.getMetadata().getDataType() != DataFieldType.DATE) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}

	// SET DECIMAL VALUE
	@TLFunctionAnnotation("Sets the decimal value of a field")
	public static final void setDecimalValue(TLFunctionCallContext context, DataRecord record, int fieldIndex, BigDecimal value) {
		DataField field = record.getField(fieldIndex);
		if (field.getMetadata().getDataType() != DataFieldType.DECIMAL) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}
	
	@TLFunctionAnnotation("Sets the decimal value of a field")
	public static final void setDecimalValue(TLFunctionCallContext context, DataRecord record, String fieldName, BigDecimal value) {
		DataField field = record.getField(fieldName);
		if (field.getMetadata().getDataType() != DataFieldType.DECIMAL) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}

	// SET INT VALUE
	@TLFunctionAnnotation("Sets the integer value of a field")
	public static final void setIntValue(TLFunctionCallContext context, DataRecord record, int fieldIndex, Integer value) {
		DataField field = record.getField(fieldIndex);
		if (field.getMetadata().getDataType() != DataFieldType.INTEGER) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}
	
	@TLFunctionAnnotation("Sets the integer value of a field")
	public static final void setIntValue(TLFunctionCallContext context, DataRecord record, String fieldName, Integer value) {
		DataField field = record.getField(fieldName);
		if (field.getMetadata().getDataType() != DataFieldType.INTEGER) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}

	// SET LONG VALUE
	@TLFunctionAnnotation("Sets the long value of a field")
	public static final void setLongValue(TLFunctionCallContext context, DataRecord record, int fieldIndex, Long value) {
		DataField field = record.getField(fieldIndex);
		if (field.getMetadata().getDataType() != DataFieldType.LONG) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}
	
	@TLFunctionAnnotation("Sets the long value of a field")
	public static final void setLongValue(TLFunctionCallContext context, DataRecord record, String fieldName, Long value) {
		DataField field = record.getField(fieldName);
		if (field.getMetadata().getDataType() != DataFieldType.LONG) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}

	// SET NUM VALUE
	@TLFunctionAnnotation("Sets the number value of a field")
	public static final void setNumValue(TLFunctionCallContext context, DataRecord record, int fieldIndex, Double value) {
		DataField field = record.getField(fieldIndex);
		if (field.getMetadata().getDataType() != DataFieldType.NUMBER) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}
	
	@TLFunctionAnnotation("Sets the number value of a field")
	public static final void setNumValue(TLFunctionCallContext context, DataRecord record, String fieldName, Double value) {
		DataField field = record.getField(fieldName);
		if (field.getMetadata().getDataType() != DataFieldType.NUMBER) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}

	// SET STRING VALUE
	@TLFunctionAnnotation("Sets the string value of a field")
	public static final void setStringValue(TLFunctionCallContext context, DataRecord record, int fieldIndex, String value) {
		DataField field = record.getField(fieldIndex);
		if (field.getMetadata().getDataType() != DataFieldType.STRING) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}
	
	@TLFunctionAnnotation("Sets the string value of a field")
	public static final void setStringValue(TLFunctionCallContext context, DataRecord record, String fieldName, String value) {
		DataField field = record.getField(fieldName);
		if (field.getMetadata().getDataType() != DataFieldType.STRING) {
			throw new RuntimeException("Illegal set function for field " + field.getMetadata().getDataType().getName());
		}
		setFieldValue(field, value);
	}

	// IS NULL
	@TLFunctionAnnotation("Returns true if a field is null")
	public static final Boolean isNull(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return record.getField(fieldIndex).isNull();
	}
	
	@TLFunctionAnnotation("Returns true if a field is null")
	public static final Boolean isNull(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return record.getField(fieldName).isNull();
	}
	
	private static final String getValueAsString(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof byte[]) {
			return ConvertLib.byte2hex(null, (byte[]) value);
		}
		return String.valueOf(value);
	}

	// GET VALUE AS STRING
	@TLFunctionAnnotation("Returns the value of a field as a string")
	public static final String getValueAsString(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return getValueAsString(getFieldValue(record, fieldIndex));
	}
	
	@TLFunctionAnnotation("Returns the value of a field as a string")
	public static final String getValueAsString(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return getValueAsString(getFieldValue(record, fieldName));
	}

	// COMPARE
	@SuppressWarnings({"rawtypes", "unchecked"})
	private static final int compare(Object value1, Object value2) {
		if (value1 == null) {
			throw new NullPointerException("value1");
		} else if (value2 == null) {
			throw new NullPointerException("value2");
		}
		if (!value1.getClass().equals(value2.getClass())) {
			throw new IllegalArgumentException("Comparing values of different types");
		}
		// TODO how about byte[] fields?
		return ((Comparable) value1).compareTo(value2);
	}
	
	@TLFunctionAnnotation("Compares the value of record1.index1 with record2.index2. The values must be comparable. Returns a negative integer, zero, or a positive integer as the first value is less than, equal to, or greater than second value.")
	public static final int compare(TLFunctionCallContext context, DataRecord record1, int fieldIndex1, DataRecord record2, int fieldIndex2) {
		Object value1 = getFieldValue(record1, fieldIndex1);
		Object value2 = getFieldValue(record2, fieldIndex2);
		return compare(value1, value2);
	}
	
	@TLFunctionAnnotation("Compares the value of record1.name1 with record2.name2. The values must be comparable. Returns a negative integer, zero, or a positive integer as the first value is less than, equal to, or greater than second value.")
	public static final int compare(TLFunctionCallContext context, DataRecord record1, String fieldName1, DataRecord record2, String fieldName2) {
		Object value1 = getFieldValue(record1, fieldName1);
		Object value2 = getFieldValue(record2, fieldName2);
		return compare(value1, value2);
	}
	
	// GET FIELD INDEX
	@TLFunctionAnnotation("Returns the index of a field")
	public static final Integer getFieldIndex(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return record.getMetadata().getFieldPosition(fieldName);
	}
	
	// GET FIELD LABEL
	@TLFunctionAnnotation("Returns the label of a field")
	public static final String getFieldLabel(TLFunctionCallContext context, DataRecord record, int fieldIndex) {
		return record.getMetadata().getField(fieldIndex).getLabelOrName();
	}

	@TLFunctionAnnotation("Returns the label of a field")
	public static final String getFieldLabel(TLFunctionCallContext context, DataRecord record, String fieldName) {
		return record.getMetadata().getField(fieldName).getLabelOrName();
	}

	class GetValueFunction implements TLFunctionPrototype {
		
		private Class<?> targetType;
		
		public GetValueFunction(Class<?> targetType) {
			this.targetType = targetType;
		}
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			DataField field = null; 
			if (context.getParams()[1].isInteger()) {
				int index = stack.popInt();
				DataRecord record = stack.popRecord();
				field = record.getField(index);
			} else if (context.getParams()[1].isString()) {
				String name = stack.popString();
				DataRecord record = stack.popRecord();
				field = record.getField(name);
			}
			Object value = getFieldValue(field);
			if (!targetType.isInstance(value)) {
				String valueStr = (value instanceof byte[]) ? Arrays.toString((byte[]) value) : String.valueOf(value);
				throw new ClassCastException("Unable to cast " + valueStr + " to " + targetType.getCanonicalName());
			}
			stack.push(targetType.cast(value));
		}
	}
	
	abstract class SetValueFunction<T> implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		@SuppressWarnings("unchecked")
		public void execute(Stack stack, TLFunctionCallContext context) {
			T value = (T) stack.pop();
			if (context.getParams()[1].isInteger()) {
				int index = stack.popInt();
				DataRecord record = stack.popRecord();
				setFieldValueIndex(context, record, index, value);
			} else if (context.getParams()[1].isString()) {
				String name = stack.popString();
				DataRecord record = stack.popRecord();
				setFieldValueName(context, record, name, value);
			}
		}
		
		protected abstract void setFieldValueIndex(TLFunctionCallContext context, DataRecord record, int fieldIndex, T value);
		protected abstract void setFieldValueName(TLFunctionCallContext context, DataRecord record, String fieldName, T value);
	}

	class IsNullFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			DataField field = null; 
			if (context.getParams()[1].isInteger()) {
				int index = stack.popInt();
				DataRecord record = stack.popRecord();
				field = record.getField(index);
			} else if (context.getParams()[1].isString()) {
				String name = stack.popString();
				DataRecord record = stack.popRecord();
				field = record.getField(name);
			} else {
				throw new IllegalArgumentException("Field can be referenced either by index[integer] or by name[string], data type '" + context.getParams()[1].toString() + "' is not supported.");
			}
			stack.push(field.isNull());
		}
	}

	class GetValueAsStringFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			Object value = null; 
			if (context.getParams()[1].isInteger()) {
				int index = stack.popInt();
				DataRecord record = stack.popRecord();
				value = getFieldValue(record, index);
			} else if (context.getParams()[1].isString()) {
				String name = stack.popString();
				DataRecord record = stack.popRecord();
				value = getFieldValue(record, name);
			}
			stack.push(getValueAsString(value));
		}
	}

	class CompareFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[1].isInteger()) {
				int index2 = stack.popInt();
				DataRecord record2 = stack.popRecord();
				int index1 = stack.popInt();
				DataRecord record1 = stack.popRecord();
				stack.push(compare(context, record1, index1, record2, index2));
			} else if (context.getParams()[1].isString()) {
				String name2 = stack.popString();
				DataRecord record2 = stack.popRecord();
				String name1 = stack.popString();
				DataRecord record1 = stack.popRecord();
				stack.push(compare(context, record1, name1, record2, name2));
			}
		}
	}

	class GetFieldIndexFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String fieldName = stack.popString();
			DataRecord record = stack.popRecord();
			stack.push(getFieldIndex(context, record, fieldName));
		}
	}

	class GetFieldLabelFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[1].isInteger()) {
				int index = stack.popInt();
				DataRecord record = stack.popRecord();
				stack.push(getFieldLabel(context, record, index));
			} else if (context.getParams()[1].isString()) {
				String name = stack.popString();
				DataRecord record = stack.popRecord();
				stack.push(getFieldLabel(context, record, name));
			}
		}
	}
}
