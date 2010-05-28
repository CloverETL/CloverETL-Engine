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
import org.jetel.metadata.DataFieldMetadata;

/**
 *  Factory Pattern which creates different types of DataField objects (subclasses) 
 *  based on specified data field type.
 *
 * @author     dpavlis
 * @since    May 2, 2002
 */
public class DataFieldFactory {

	/**
	 *  Factory which creates data field based on specified field type and metadata.
	 * You should use this method whenever you want to create a data field.
	 *
	 * @param  fieldType      One of the recognized Data Field Types
	 * @param  fieldMetadata  metadata reference
	 * @return                new data field object
	 * @since                 May 2, 2002
	 */
	public final static DataField createDataField(char fieldType, DataFieldMetadata fieldMetadata,boolean plain) {
		switch (fieldType) {
			case DataFieldMetadata.STRING_FIELD:
				return new StringDataField(fieldMetadata,plain);
			case DataFieldMetadata.DATE_FIELD:
				return new DateDataField(fieldMetadata,plain);
			case DataFieldMetadata.NUMERIC_FIELD:
				return new NumericDataField(fieldMetadata,plain);
			case DataFieldMetadata.DECIMAL_FIELD:
                return new DecimalDataField(fieldMetadata, fieldMetadata.getFieldProperties().getIntProperty(DataFieldMetadata.LENGTH_ATTR), fieldMetadata.getFieldProperties().getIntProperty(DataFieldMetadata.SCALE_ATTR), false);
			case DataFieldMetadata.INTEGER_FIELD:
				return new IntegerDataField(fieldMetadata,plain);
			case DataFieldMetadata.BYTE_FIELD:
				return new ByteDataField(fieldMetadata,plain);
			case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
				return new CompressedByteDataField(fieldMetadata,plain);
			case DataFieldMetadata.LONG_FIELD:
				return new LongDataField(fieldMetadata,plain);
			case DataFieldMetadata.BOOLEAN_FIELD:
				return new BooleanDataField(fieldMetadata);
			case DataFieldMetadata.NULL_FIELD:
				return NullField.NULL_FIELD;
			default:
				throw new RuntimeException("Unsupported data type: " + fieldType);
		}
	}

	
	/**
	 * Simplified version of previous. Gets field type from metadata.
	 * 
	 * @param fieldMetadata Metadata describing field's characteristics (eg. data-type, size)
	 * @return
	 */
	public final static DataField createDataField(DataFieldMetadata fieldMetadata,boolean plain){
		return createDataField(fieldMetadata.getType(),fieldMetadata,plain);
	}
	
	
	/**
	 * @param c
	 * @param fieldMetadata
	 * @param object
	 * @param sequencedDependencies
	 * @return
	 */
	public static DataField createDataField(char c, DataFieldMetadata fieldMetadata, String methodName, int[][] sequencedDependencies) {
		// TODO Auto-generated method stub
		return null;
	}

}

