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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.CreateJavaObject;

/**
 * The purpose for this class is to calculate the value of the field
 * based on code property
 * 
 * @author Wes Maciorowski
 * @version 1.0
 *
 */
public class CodeField extends DataField {
	private DataField aDataField = null;
	private String className = null;
	private String methodName = null;

	/** it is an array of tuples where the first element is record's position
	 * in inputDataRecords and second element is field position in its record.
	 */
	private int[][] parmLocations = null;

	/**
	 * @param _metadata
	 */
	public CodeField(DataFieldMetadata _metadata, DataField aDataField) {
		super(_metadata);
		this.aDataField = aDataField;
	}

	/**
	 * This method calculates the value of the field.
	 * @param inputDataRecords - an array of records from all
	 *                           input nodes
	 * @return fields calculated value
	 */
	public Object calculate(DataRecord[] inputDataRecords) {
		Object[] anObj = null;
		anObj = new Object[parmLocations.length];
		for (int i = 0; i < parmLocations.length; i++) {
			anObj[i] =
				inputDataRecords[parmLocations[i][0]]
					.getField(parmLocations[i][1])
					.getValue();
		}

		//Integer tmpInteger = (Integer) CreateJavaObject.invokeMethod("com.wesm.brain.util.test.test2","addTwo",anObj, null);
		// first phase we will only handle classes without costructor parameters
		return CreateJavaObject.invokeMethod(
			className,
			methodName,
			null,
			anObj);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Object arg0) {
		return aDataField.compareTo(arg0);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#deserialize(java.nio.ByteBuffer)
	 */
	public void deserialize(ByteBuffer buffer) {
		aDataField.deserialize(buffer);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		return aDataField.equals(obj);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#fromByteBuffer(java.nio.ByteBuffer, java.nio.charset.CharsetDecoder)
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder)
		throws CharacterCodingException {
		aDataField.fromByteBuffer(dataBuffer, decoder);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#fromString(java.lang.String)
	 */
	public void fromString(String _str) {
		aDataField.fromString(_str);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#getType()
	 */
	public char getType() {
		return aDataField.getType();
	}

	/** (non-Javadoc)
	 * @see org.jetel.data.DataField#getValue()
	 */
	public Object getValue() {
		return aDataField.getValue();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#serialize(java.nio.ByteBuffer)
	 */
	public void serialize(ByteBuffer buffer) {
		aDataField.serialize(buffer);
	}
	/** (non-Javadoc)
	 * @see org.jetel.data.DataField#setValue(java.lang.Object)
	 */
	public void setValue(Object _value) {
		aDataField.setValue(_value);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#toByteBuffer(java.nio.ByteBuffer, java.nio.charset.CharsetEncoder)
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder)
		throws CharacterCodingException {
		aDataField.toByteBuffer(dataBuffer, encoder);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return aDataField.toString();
	}

	public int getSizeSerialized(){
		return 0;
	}
}
