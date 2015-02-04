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
package org.jetel.data;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;

/**
 *  Factory Pattern which creates different types of DataField objects (subclasses) 
 *  based on specified data field type.
 *
 * @author     dpavlis
 * @since    May 2, 2002
 */
public abstract class DataFieldFactory {

	/**
	 * This field factory creates {@link DataFieldWithInvalidState} instead of regular {@link DataFieldImpl}.
	 */
	public static DataFieldFactory DATA_FIELD_WITH_INVALID_STATE = new DataFieldFactory() {
		@Override
		public DataField create(DataFieldType fieldType, DataFieldMetadata fieldMetadata, boolean plain) {
			DataField dataField = createDataField(fieldType, fieldMetadata, plain);
			return new DataFieldWithInvalidState(dataField);
		}
	};
	
	/**
	 * Creates an {@link DataField} instance.
	 */
	public abstract DataField create(DataFieldType fieldType, DataFieldMetadata fieldMetadata,boolean plain);
	
	/**
	 * @param fieldType
	 * @param fieldMetadata
	 * @param plain
	 * @return
	 * @deprecated use {@link #createDataField(DataFieldType, DataFieldMetadata, boolean)} instead
	 */
	@Deprecated
	public final static DataField createDataField(char fieldType, DataFieldMetadata fieldMetadata,boolean plain) {
		return createDataField(DataFieldType.fromChar(fieldType), fieldMetadata, plain);
	}

	/**
	 *  Factory which creates data field based on specified field type and metadata.
	 * You should use this method whenever you want to create a data field.
	 *
	 * @param  fieldType      One of the recognized Data Field Types
	 * @param  fieldMetadata  metadata reference
	 * @return                new data field object
	 * @since                 May 2, 2002
	 */
	public final static DataField createDataField(DataFieldType fieldType, DataFieldMetadata fieldMetadata,boolean plain) {
		DataFieldContainerType containerType = fieldMetadata.getContainerType();
		switch (containerType) {
		case SINGLE: 
			try {
				switch (fieldType) {
					case STRING:
						return new StringDataField(fieldMetadata,plain);
					case DATE:
						return new DateDataField(fieldMetadata,plain);
					case NUMBER:
						return new NumericDataField(fieldMetadata,plain);
					case DECIMAL:
		                return new DecimalDataField(fieldMetadata, fieldMetadata.getFieldProperties().getIntProperty(DataFieldMetadata.LENGTH_ATTR), fieldMetadata.getFieldProperties().getIntProperty(DataFieldMetadata.SCALE_ATTR), false);
					case INTEGER:
						return new IntegerDataField(fieldMetadata,plain);
					case BYTE:
						return new ByteDataField(fieldMetadata,plain);
					case CBYTE:
						return new CompressedByteDataField(fieldMetadata,plain);
					case LONG:
						return new LongDataField(fieldMetadata,plain);
					case BOOLEAN:
						return new BooleanDataField(fieldMetadata);
					case NULL:
						return NullField.NULL_FIELD;
					default:
						throw new RuntimeException("Unsupported data type: " + fieldType);
				}
			} catch (Exception e) {
				throw new JetelRuntimeException(String.format("Data field '%s' cannot be created.", fieldMetadata.getName()), e);
			}
		case LIST:
			return new ListDataField(fieldMetadata, plain);
		case MAP:
			return new MapDataField(fieldMetadata, plain);
		default:
			throw new RuntimeException("Unsupported field container type: " + containerType);
		}
	}
	
	/**
	 * Simplified version of previous. Gets field type from metadata.
	 * 
	 * @param fieldMetadata Metadata describing field's characteristics (eg. data-type, size)
	 * @return
	 */
	public final static DataField createDataField(DataFieldMetadata fieldMetadata,boolean plain){
		return createDataField(fieldMetadata.getDataType(),fieldMetadata,plain);
	}
	
}

