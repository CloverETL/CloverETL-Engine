/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
	 *  Description of the Method
	 *
	 * @param  fieldType      One of the recognized Data Field Types
	 * @param  fieldMetadata  metadata reference
	 * @return                new data field object
	 * @since                 May 2, 2002
	 */
	public final static DataField createDataField(char fieldType, DataFieldMetadata fieldMetadata) {
		switch (fieldType) {
			case DataFieldMetadata.STRING_FIELD:
				return new StringDataField(fieldMetadata);
			case DataFieldMetadata.DATE_FIELD:
				return new DateDataField(fieldMetadata);
			case DataFieldMetadata.NUMERIC_FIELD:
				return new NumericDataField(fieldMetadata);
			case DataFieldMetadata.INTEGER_FIELD:
				return new IntegerDataField(fieldMetadata);
			case DataFieldMetadata.BYTE_FIELD:
				return new ByteDataField(fieldMetadata);
			default:
				throw new RuntimeException("Unsupported data type: " + fieldType);
		}
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

