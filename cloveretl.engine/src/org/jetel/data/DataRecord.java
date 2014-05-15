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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 *  An interface that represents one data record with structure based on provided
 *  metadata object.<br> 
 *  Fields of this data record are created only once and updated when it is needed.<br>
 *  When we need to send record through the EDGE (or other communication channel), we just serialize it 
 *  to stream of bytes (it isn't standard Java serializing, but Clover's own).
 *  
 *  Note: this abstract class cannot be 'interfaced' due backward compatibility on byte code level 
 *
 * @author      D.Pavlis
 * @since       March 26, 2002
 * @created     May 18, 2003
 * @see         org.jetel.metadata.DataRecordMetadata
 */
public abstract class DataRecord implements Serializable, Comparable<Object>, Iterable<DataField> {

	private static final long serialVersionUID = 3312575262868262204L;

	/**
	 * Creates deep copy of existing record (field by field).
	 * 
	 * @return new DataRecord
	 */
	public abstract DataRecord duplicate();

	/**
	 * Set fields by copying the fields from the record passed as argument.
	 * Does not assume that both records have the right same structure - i.e. metadata.
	 * @param fromRecord DataRecord from which to get fields' values
	 */
	public abstract void copyFrom(DataRecord fromRecord);

	/**
	 * Set fields by copying the fields from the record passed as argument.
	 * Can handle situation when records are not exactly the same.
	 * For incompatible fields default value is setted.
	 * This operation is not intended for time critical purposes - in this case 
	 * consider to use copyFrom() method instead.
	 *
	 * @param  sourceRecord  Record from which fields are copied
	 * @since
	 */

	public abstract void copyFieldsByPosition(DataRecord sourceRecord);

	/**
	 * Set fields by copying name-matching fields' values from the record passed as argument.<br>
	 * If two fields match by name but not by type, target field is set to default value.
	 * For incompatible fields default value is set.
	 * This operation is not intended for time critical purposes - in this case 
	 * consider to use copyFrom() method instead.
	 *  
	 * @param sourceRecord from which copy data
	 * @return boolean array with true values on positions, where values were copied from source record
	 */
	public abstract boolean[] copyFieldsByName(DataRecord sourceRecord);

	/**
	 *  Deletes/removes specified field. The field's internal reference
	 * is set to NULL, so it can be garbage collected.<br>
	 * <b>Warning:</b>We recommend not using this method !<br>
	 * By calling this method, the number of fields is decreased by
	 * one, the internal array containing fields is copied into a new
	 * one.<br>
	 * Be careful when calling this method as the modified record
	 * won't follow the original metadata prescription. New metadata object
	 * will be created and assigned to this record, but it will share fields'
	 * metadata with the original metadata object.<br>
	 *
	 * @param  _fieldNum  Description of Parameter
	 * @since
	 */
	public abstract void delField(int _fieldNum);

	/**
	 *  Refreshes this record's content from ByteBuffer. 
	 *
	 * @param  buffer  ByteBuffer from which this record's fields should be read
	 * @since          April 23, 2002
	 */
	public abstract void deserialize(CloverBuffer buffer);

	/**
	 * Unitary deserialization should be compatible with
	 * unitary serialization. Moreover this type of unitary
	 * serialization and deserialization should be compatible
	 * with all descendants of DataRecord - for now it is only
	 * Token and DataRecord. So for example serialized
	 * DataRecord can be deserialized into Token and vice versa.
	 */
	public abstract void deserializeUnitary(CloverBuffer buffer);
	
	public abstract void deserializeUnitary(CloverBuffer buffer,DataRecordSerializer serializer);

	/**
	 * @deprecated use {@link #deserialize(CloverBuffer)} instead
	 */
	@Deprecated
	public abstract void deserialize(ByteBuffer buffer);

	public abstract void deserialize(CloverBuffer buffer, int[] whichFields);
	
	public abstract void deserialize(CloverBuffer buffer, DataRecordSerializer serializer);

	/**
	 * @deprecated use {@link #deserialize(CloverBuffer, int[])} instead
	 */
	@Deprecated
	public abstract void deserialize(ByteBuffer buffer, int[] whichFields);

	/**
	 *  Test two DataRecords for equality. Records must have the same metadata (be
	 * created using the same metadata object) and their field values must be equal.
	 *
	 * @param  obj  DataRecord to compare with
	 * @return      True if they equals, false otherwise
	 * @since       April 23, 2002
	 */
	@Override
	public abstract boolean equals(Object obj);

	/**
	 * Compares two DataRecords. Records must have the same metadata (be
	 * created using the same metadata object). Their field values are compare one by one,
	 * the first non-equal pair of fields denotes the overall comparison result.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public abstract int compareTo(Object obj);

	/**
	 *  An operation that returns DataField with
	 *  specified order number.
	 *
	 * @param  _fieldNum  Description of Parameter
	 * @return            The Field value
	 * @since
	 */
	public abstract DataField getField(int _fieldNum);

	/**
	 *  An operation that returns DataField with
	 *  specified name.
	 *
	 * @param  _name  Description of Parameter
	 * @return        The Field value
	 * @since
	 */
	public abstract DataField getField(String _name);

	/**
	 *  An operation that returns DataField with
	 *  specified label.
	 *
	 * @param  _name  Description of Parameter
	 * @return        The Field value
	 * @since
	 */
	public abstract DataField getFieldByLabel(String _label);

	
	/**
	 * An operation that returns all fields composing
	 * this record.
	 * 
	 * @return array data fields 
	 */
	public abstract DataField[] getFields();
	
	
	/**
	 * Returns true if record contains a field with a given name.
	 * @param name
	 * @return
	 */
	public abstract boolean hasField(String name);

	/**
	 * Returns true if record contains a field with a given label.
	 * @param label
	 * @return
	 */
	public abstract boolean hasLabeledField(String label);

	/**
	 *  An attribute that returns metadata object describing the record
	 *
	 * @return    The Metadata value
	 * @since
	 */
	public abstract DataRecordMetadata getMetadata();

	/**
	 *  An operation that returns numbe of fields this record
	 *  contains.
	 *
	 * @return    The NumFields value
	 * @since
	 */
	public abstract int getNumFields();

	/**
	 * Initializes the record - creates its fields.
	 * 
	 * <p>This method has to be called before accessing any of the record's fields.
	 *
	 * @since    April 5, 2002
	 */
	public abstract void init();

	/**
	 *  Serializes this record's content into ByteBuffer.
	 *
	 * @param  buffer  ByteBuffer into which the individual fields of this record should be put
	 * @since          April 23, 2002
	 */
	public abstract void serialize(CloverBuffer buffer);
	
	public abstract void serialize(CloverBuffer buffer,DataRecordSerializer serializer);

	/**
	 * Unitary deserialization should be compatible with
	 * unitary serialization. Moreover this type of unitary
	 * serialization and deserialization should be compatible
	 * with all descendants of DataRecord - for now it is only
	 * Token and DataRecord. So for example serialized
	 * DataRecord can be deserialized into Token and vice versa.
	 */
	public abstract void serializeUnitary(CloverBuffer buffer);
	
	public abstract void serializeUnitary(CloverBuffer buffer,DataRecordSerializer serializer);
	
	/**
	 * @deprecated use {@link #serialize(CloverBuffer)} instead
	 */
	@Deprecated
	public abstract void serialize(ByteBuffer buffer);

	/**
	 * Serializes this record's content into ByteBuffer.<br>
	 * Asume only fields which indexes are in fields array
	 * 
	 * @param buffer
	 * @param whichFields
	 * @since 27.2.2007
	 */
	public abstract void serialize(CloverBuffer buffer, int[] whichFields);

	/**
	 * @deprecated {@link #serialize(CloverBuffer, int[])} instead
	 */
	@Deprecated
	public abstract void serialize(ByteBuffer buffer, int[] whichFields);

	/**
	 *  Assigns new metadata to this DataRecord. If the new
	 * metadata is not equal to the current metadata, the record's
	 * content is recreated from scratch. After calling this
	 * method, record is uninitialized and init() method should
	 * be called prior any attempt to manipulate this record's content.
	 *
	 * @param  metadata  The new Metadata value
	 * @since            April 5, 2002
	 */
	public abstract void setMetadata(DataRecordMetadata metadata);

	/**
	 *  An operation that sets value of all data fields to their default value.
	 */
	public abstract void setToDefaultValue();

	/**
	 *  An operation that sets value of the selected data field to its default
	 *  value.
	 *
	 * @param  _fieldNum  Ordinal number of the field which should be set to default
	 */
	public abstract void setToDefaultValue(int _fieldNum);

	/**
	 *  An operation that sets value of all data fields to NULL value.
	 */
	public abstract void setToNull();

	/**
	 *  An operation that sets value of the selected data field to NULL
	 *  value.
	 * @param _fieldNum
	 */
	public abstract void setToNull(int _fieldNum);

	/**
	 * An operation which sets/resets all fields to their
	 * initial value (just after they were created by JVM) - 
	 * it varies depending on type of field.<br>
	 * Nullable fields are set to NULL, non-nullable are zeroed, if they
	 * have default value defined, then default value is assigned.
	 */
	public abstract void reset();

	/**
	 * An operation that resets value of specified field - by calling
	 * its reset() method.
	 * 
	 * @param _fieldNum Ordinal number of the field which should be set to default
	 */
	public abstract void reset(int _fieldNum);

	/**
	 *  Creates textual representation of record's content based on values of individual
	 *  fields
	 *
	 * @return    Description of the Return Value
	 */
	@Override
	public abstract String toString();

	/**
	 *  Gets the actual size of record (in bytes).<br>
	 *  <i>How many bytes are required for record serialization</i>
	 *
	 * @return    The size value
	 */
	public abstract int getSizeSerialized();

	/**
	 * Size of unitary serialization form of this data record.
	 * Unitary deserialization should be compatible with
	 * unitary serialization. Moreover this type of unitary
	 * serialization and deserialization should be compatible
	 * with all descendants of DataRecord - for now it is only
	 * Token and DataRecord. So for example serialized
	 * DataRecord can be deserialized into Token and vice versa.
	 */
	public abstract int getSizeSerializedUnitary();

	/**
	 * Test whether the whole record has NULL value - i.e.
	 * every field it contains has NULL value.
	 * @return true if all fields have NULL value otherwise false
	 */

	public abstract boolean isNull();

	/**
	 * Returns iterator over all contained data fields.
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public abstract Iterator<DataField> iterator();

}