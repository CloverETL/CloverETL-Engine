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
// FILE: c:/projects/jetel/org/jetel/data/DataRecord.java

package org.jetel.data;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.jetel.exception.ClassCompilationException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ClassBuilder;
import org.jetel.util.CloverProperties;
import org.jetel.util.Compile;

/**
 *  A class that represents one data record Fields are not deleted & created all
 *  the time. Instead, they are created only once and updated when it is needed
 *  When we need to send record through the EDGE, we just serialize it (it isn't
 *  standard version of serializing)
 *
 * @author      D.Pavlis
 * @since       March 26, 2002
 * @revision    $Revision$
 * @created     18. kvìten 2003
 * @see         org.jetel.metadata.DataRecordMetadata
 */
public class DataRecord implements Serializable {

	/**
	 * @since
	 */
	private transient String codeClassName;

	/**
	 * @since
	 */
	private DataField fields[];

	// Associations
	/**
	 * @since
	 */
	private transient DataRecordMetadata metadata;

	// Attributes
	/**
	 *  An attribute that represents ...
	 *
	 * @param  _metadata  Description of Parameter
	 * @since
	 */

	// Operations

	public DataRecord(DataRecordMetadata _metadata) {
		this.metadata = _metadata;
		fields = new DataField[metadata.getNumFields()];
	}


	/**
	 *  Set fields by copying the fields from the record passed as argument.
	 *
	 *
	 * @param  _record  Record from which fields are copied
	 * @since
	 */

	public void copyFieldsByPosition(DataRecord _record) {
		DataRecordMetadata sourceMetadata = _record.getMetadata();
		DataFieldMetadata fieldMetadata;
		DataField sourceField;
		DataField targetField;
		int sourceLength = sourceMetadata.getNumFields();
		int targetLength = this.metadata.getNumFields();
		int copyLength;
		if (sourceLength < targetLength) {
			copyLength = sourceLength;
		} else {
			copyLength = targetLength;
		}

		for (int i = 0; i < copyLength; i++) {

			fieldMetadata = metadata.getField(i);
			sourceField = _record.getField(i);
			targetField = this.getField(i);
			if (sourceField.getType() == targetField.getType()) {
				targetField.setValue(sourceField.getValue());
			} else {
				targetField.setToDefaultValue();
			}
		}
	}





	/**
	 *  Description of the Method
	 *
	 * @param  _fieldNum  Description of Parameter
	 * @since
	 */
	public void delField(int _fieldNum) {
		try {
			fields[_fieldNum] = null;
		} catch (IndexOutOfBoundsException e) {
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		for (int i = 0; i < fields.length; i++) {
			fields[i].deserialize(buffer);
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  obj  Description of Parameter
	 * @return      Description of the Returned Value
	 * @since       April 23, 2002
	 */
	public boolean equals(Object obj) {
		/*
		 *  first test that both records have the same structure i.e. point to the same
		 *  metadata
		 */
		if (metadata != ((DataRecord) obj).getMetadata()) {
			return false;
		}
		// check field by field that they are the same
		for (int i = 0; i < fields.length; i++) {
			if (!fields[i].equals(((DataRecord) obj).getField(i))) {
				return false;
			}
		}
		return true;
	}


	/**
	 *  Gets the codeClassName attribute of the DataRecord object
	 *
	 * @return    The codeClassName value
	 */
	public String getCodeClassName() {
		return codeClassName;
	}


	/**
	 *  An operation that does ...
	 *
	 * @param  _fieldNum  Description of Parameter
	 * @return            The Field value
	 * @since
	 */
	public DataField getField(int _fieldNum) {
		try {
			return fields[_fieldNum];
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}


	/**
	 *  An operation that does ...
	 *
	 * @param  _name  Description of Parameter
	 * @return        The Field value
	 * @since
	 */
	public DataField getField(String _name) {
		try {
			return fields[metadata.getFieldPosition(_name)];
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}


	/**
	 *  An attribute that represents ... An operation that does ...
	 *
	 * @return    The Metadata value
	 * @since
	 */
	public DataRecordMetadata getMetadata() {
		return metadata;
	}


	/**
	 *  An operation that does ...
	 *
	 * @return    The NumFields value
	 * @since
	 */
	public int getNumFields() {
		return metadata.getNumFields();
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 5, 2002
	 */
	public void init() {
		DataFieldMetadata fieldMetadata;
		// create appropriate data fields based on metadata supplied
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldMetadata = metadata.getField(i);
			fields[i] =
					DataFieldFactory.createDataField(
					fieldMetadata.getType(),
					fieldMetadata);
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void serialize(ByteBuffer buffer) {
		for (int i = 0; i < fields.length; i++) {
			fields[i].serialize(buffer);
		}
	}


	/**
	 *  Sets the codeClassName attribute of the DataRecord object
	 *
	 * @param  codeClassName  The new codeClassName value
	 */
	public void setCodeClassName(String codeClassName) {
		this.codeClassName = codeClassName;
	}


	/**
	 *  Sets the Metadata attribute of the DataRecord object
	 *
	 * @param  metadata  The new Metadata value
	 * @since            April 5, 2002
	 */
	public void setMetadata(DataRecordMetadata metadata) {
		this.metadata = metadata;
	}


	/**
	 *  An operation that sets value of all data fields to their default value.
	 */
	public void setToDefaultValue() {
		for (int i = 0; i < fields.length; i++) {
			fields[i].setToDefaultValue();
		}
	}


	/**
	 *  An operation that sets value of the selected data field to its default
	 *  value.
	 *
	 * @param  _fieldNum  The new toDefaultValue value
	 */
	public void setToDefaultValue(int _fieldNum) {
		fields[_fieldNum].setToDefaultValue();
	}


	/**
	 *  Creates textual representation of record's content based on values of individual
	 *  fields
	 *
	 * @return    Description of the Return Value
	 */
	public String toString() {
		StringBuffer str = new StringBuffer();
		for (int i = 0; i < fields.length; i++) {
			str.append("#").append(i).append("->");
			str.append(fields[i].toString());
			str.append("\n");
		}
		return str.toString();
	}
}
/*
 *  end class DataRecord
 */

