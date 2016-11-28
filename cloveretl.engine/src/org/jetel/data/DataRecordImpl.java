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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.HashCodeUtil;
import org.jetel.util.bytes.CloverBuffer;

/**
 *  A class that represents one data record with structure based on provided
 *  metadata object.<br> 
 *  Fields of this data record are created only once and updated when it is needed.<br>
 *  When we need to send record through the EDGE (or other communication channel), we just serialize it 
 *  to stream of bytes (it isn't standard Java serializing, but Clover's own).
 *
 * @author      D.Pavlis
 * @since       March 26, 2002
 * @created     May 18, 2003
 * @see         org.jetel.metadata.DataRecordMetadata
 */
public class DataRecordImpl extends DataRecord {

	/** The most significant bit in a byte */
	private static final int HIGHEST_BIT = 0x80;

    /**
     * Array for holding data fields
     * 
	 * @since
	 */
	private DataField[] fields;

	/**
     * Reference to metadata object describing this record 
	 * @since
	 */
	private DataRecordMetadata metadata;

    
    /**
     * Should this record and all its fields be created in plain mode ?<br>
     * Plain means no "decorators" will be added and this record is deemed to
     * store values only, no formating or parsing will be performed on this
     * record.
     */
    private boolean plain=false;
    
    /**
     * If set to <code>false</code>, deserialization will skip auto-filled fields.
     * 
     * <code>true</code> by default.
     * 
     * @see DataRecord#setDeserializeAutofilledFields(boolean)
     * @see <a href="https://bug.javlin.eu/browse/CLO-4591">CLO-4591</a>
     */
    private boolean deserializeAutofilledFields = true;
    
    /**
     * Indicates the record has been already initialized (see {@link #init()} method).
     */
    private boolean isInitialized = false;
    
	/**
	 * Create new instance of DataRecord based on specified metadata (
	 * how many fields, what field types, etc.)
	 * 
	 * @param _metadata  description of the record structure
     * @deprecated use factory method {@link DataRecordFactory#newRecord(DataRecordMetadata)} instead
	 */
    @Deprecated
	public DataRecordImpl(DataRecordMetadata _metadata) {
		this(_metadata,false);
	}
    
    /**
     * Create new instance of DataRecord based on specified metadata (
     * how many fields, what field types, etc.)
     * 
     * @param _metadata description of the record structure
     * @param plain if true, no formatters and other "extra" objects will be created for
     * fields
     * @deprecated use factory method {@link DataRecordFactory#newRecord(DataRecordMetadata)} instead
     */
    @Deprecated
    public DataRecordImpl(DataRecordMetadata _metadata,boolean plain ) {
        this.metadata = _metadata;
        this.plain=plain;
        fields = new DataField[metadata.getNumFields()];
    }

	/**
	 * Creates deep copy of existing record (field by field).
	 */
	@Override
	public DataRecordImpl duplicate() {
		return duplicate(null);
	}
	
	/**
	 * Creates deep copy of existing record - only fields from the given key are considered.
	 * RecordKey can be null, all fields are duplicated, see {@link #duplicate()}.
	 */
	@Override
	public DataRecordImpl duplicate(RecordKey recordKey) {
		DataRecordMetadata keyMetadata = recordKey != null ? recordKey.getKeyRecordMetadata() : metadata;
		DataRecordImpl keyRecord = newInstance(keyMetadata);
		int keyFieldIndex = 0;
		for (int i = 0; i < fields.length; i++) {
			if (recordKey == null || recordKey.isKeyField(i)) {
				keyRecord.fields[keyFieldIndex++] = fields[i].duplicate();
			}
		}
		return keyRecord;
	}
	
	/**
	 * Creates new instance of this class. This method should be overridden in descendants.
	 * @param metadata metadata of created record
	 * @return new instance of this class
	 * @see #duplicate()
	 */
	protected DataRecordImpl newInstance(DataRecordMetadata metadata) {
		return new DataRecordImpl(metadata);
	}
	
	/**
	 * Set fields by copying the fields from the record passed as argument.
	 * Does not assume that both records have the right same structure - i.e. metadata.
	 * @param fromRecord DataRecord from which to get fields' values
	 */
	@Override
	public void copyFrom(DataRecord fromRecord){
        int length = Math.min(fields.length, fromRecord.getNumFields());
        
	    for (int i = 0; i < length; i++){
	        this.fields[i].setValue(fromRecord.getField(i));
	    } 
	}
	
	
	/**
	 * Set fields by copying the fields from the record passed as argument.
	 * Can handle situation when records are not exactly the same.
     * For incompatible fields default value is set (except on decimals).
     * This operation is not intended for time critical purposes - in this case 
     * consider to use copyFrom() method instead.
	 *
	 * @param  sourceRecord  Record from which fields are copied
	 * @since
	 */
	@Override
	public void copyFieldsByPosition(DataRecord sourceRecord) {
		if (sourceRecord == null) {
			reset();
			return;
		}
		int copyLength;
        DataField sourceField;
        DataField targetField;
        
		copyLength = Math.min(sourceRecord.getNumFields(), fields.length);
        
		for (int i = 0; i < copyLength; i++) {
			//fieldMetadata = metadata.getField(i);
			sourceField = sourceRecord.getField(i);
			targetField = this.getField(i);
			if (targetField.getMetadata().isSubtype(sourceField.getMetadata()) ||
            		(sourceField.getMetadata().getDataType() == DataFieldType.DECIMAL && targetField.getMetadata().getDataType() == DataFieldType.DECIMAL)) {
				// fix CLO-9831 : decimals are be mapped by name even if their precision doesn't fit
				targetField.setValue(sourceField);
			} else {
				targetField.setToDefaultValue();
			}
		}
	}
	
	/**
	 * Set fields by copying name-matching fields' values from the record passed as argument.<br>
     * If two fields match by name but not by type, target field is set to default value.
     * For incompatible fields default value is set (except on decimals).
     * This operation is not intended for time critical purposes - in this case 
     * consider to use copyFrom() method instead.
	 *  
	 * @param sourceRecord from which copy data
	 * @return boolean array with true values on positions, where values were copied from source record
	 */
	@Override
	public boolean[] copyFieldsByName(DataRecord sourceRecord) {
        boolean[] result = new boolean[fields.length];
        Arrays.fill(result, false);
        if (sourceRecord == null) return result;
        DataField sourceField;
        DataField targetField;
        int sourceLength = sourceRecord.getMetadata().getNumFields();
        int count = 0;
        for (int i = 0; i < fields.length; i++) {
            targetField = getField(i);
            int srcFieldPos = sourceRecord.getMetadata().getFieldPosition(
                    targetField.getMetadata().getName());
            if (srcFieldPos >= 0) {
                sourceField = sourceRecord.getField(srcFieldPos);
                result[i] = true;
                if (sourceField.getMetadata().isSubtype(targetField.getMetadata()) ||
                		(sourceField.getMetadata().getDataType() == DataFieldType.DECIMAL && targetField.getMetadata().getDataType() == DataFieldType.DECIMAL)) {
                	// fix CLO-9831 : decimals are be mapped by name even if their precision doesn't fit
                    targetField.setValue(sourceField);
                } else {
                    targetField.setToDefaultValue();
                }
                if (count++ > sourceLength)
                    break;//all fields from source were used
            }
        }
        return result;
    }




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
	@Deprecated
	@Override
	public void delField(int _fieldNum) {
            DataField tmp_fields[]=new DataField[fields.length-1];
            DataRecordMetadata tmp_metadata = new DataRecordMetadata(metadata.getName(),metadata.getParsingType());
            tmp_metadata.setLabel(metadata.getLabel());
            int counter=0;
            for(int i=0;i<fields.length;i++){
                if (i!=_fieldNum){
                    tmp_fields[counter]=fields[i];
                    tmp_metadata.addField(tmp_fields[counter].getMetadata());
                    counter++;
                }
            }
            fields=tmp_fields;
            metadata=tmp_metadata;
	}


	/**
	 *  Refreshes this record's content from ByteBuffer. 
	 *
	 * @param  buffer  ByteBuffer from which this record's fields should be read
	 * @since          April 23, 2002
	 */
	@Override
	public void deserialize(CloverBuffer buffer) {
		if (deserializeAutofilledFields) {
			for (DataField field : fields) {
				field.deserialize(buffer);
			}
		} else { // CLO-4591
			deserialize(buffer, metadata.getNonAutofilledFields());
		}
	}
	
	@Override
	public void deserialize(CloverBuffer buffer, DataRecordSerializer serializer){
		if (deserializeAutofilledFields) {
			serializer.deserialize(buffer,this);
		} else { // CLO-4591
			serializer.deserialize(buffer,this, metadata.getNonAutofilledFields());
		}
	}

	/**
	 * Unitary deserialization should be compatible with
	 * unitary serialization. Moreover this type of unitary
	 * serialization and deserialization should be compatible
	 * with all descendants of DataRecord - for now it is only
	 * Token and DataRecord. So for example serialized
	 * DataRecord can be deserialized into Token and vice versa.
	 */
	@Override
	public void deserializeUnitary(CloverBuffer buffer) {
		this.deserialize(buffer);
	}
	
	@Override
	public void deserializeUnitary(CloverBuffer buffer, DataRecordSerializer serializer) {
		this.deserialize(buffer,serializer);
	}

	/**
	 * @deprecated use {@link #deserialize(CloverBuffer)} instead
	 */
	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public void deserialize(ByteBuffer buffer) {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(buffer);
		deserialize(wrappedBuffer);
		if (wrappedBuffer.buf() != buffer) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
	}

    @Override
	public void deserialize(CloverBuffer buffer,int[] whichFields) {
        for(int i:whichFields){
            fields[i].deserialize(buffer);
        }
    }
    
    /**
     * @deprecated use {@link #deserialize(CloverBuffer, int[])} instead
     */
    @SuppressWarnings("deprecation")
	@Override
	@Deprecated
    public void deserialize(ByteBuffer buffer, int[] whichFields) {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(buffer);
		deserialize(wrappedBuffer, whichFields);
		if (wrappedBuffer.buf() != buffer) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
    }

	/**
	 *  Test two DataRecords for equality. Records must have the same metadata (be
	 * created using the same metadata object) and their field values must be equal.
	 *
	 * @param  obj  DataRecord to compare with
	 * @return      True if they equals, false otherwise
	 * @since       April 23, 2002
	 */
	@Override
	public boolean equals(Object obj) {
		if (this==obj) return true;
	    
	    /*
         * first test that both records have the same structure i.e. point to
         * the same metadata
         */
        if (obj instanceof DataRecord) {
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
        }else{
            return false;
        }
    }

	
	/**
	 * Compares two DataRecords. Records must have the same metadata (be
	 * created using the same metadata object). Their field values are compare one by one,
	 * the first non-equal pair of fields denotes the overall comparison result.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Object obj){
	    if (this==obj) return 0;
	    
	    if (obj instanceof DataRecord) {
	    	if (this instanceof NullRecord) return -1;
	    	if (obj instanceof NullRecord) return 1;
	    	
            if (metadata != ((DataRecord) obj).getMetadata()) {
                throw new RuntimeException("Can't compare - records have different metadata objects assigned!");
            }
            int cmp;
            // check field by field that they are the same
            for (int i = 0; i < fields.length; i++) {
                cmp=fields[i].compareTo(((DataRecord) obj).getField(i));
                if (cmp!=0) {
                    return cmp;
                }
            }
            return 0;
        }else{
            throw new ClassCastException("Can't compare DataRecord with "+obj.getClass().getName());
        }
	}


	/**
	 *  An operation that returns DataField with
     *  specified order number.
	 *
	 * @param  _fieldNum  Description of Parameter
	 * @return            The Field value
	 * @since
	 */
	@Override
	public DataField getField(int _fieldNum) {
	    return fields[_fieldNum];
	}


	/**
	 *  An operation that returns DataField with
     *  specified name.
	 *
	 * @param  _name  Description of Parameter
	 * @return        The Field value
	 * @since
	 */
	@Override
	public DataField getField(String _name) {
	    return fields[metadata.getFieldPosition(_name)];
	}

	/**
	 *  An operation that returns DataField with
     *  specified label.
	 *
	 * @param  _name  Description of Parameter
	 * @return        The Field value
	 * @since
	 */
	@Override
	public DataField getFieldByLabel(String _label) {
		return fields[metadata.getFieldPositionByLabel(_label)];
	}
	
	@Override
	public DataField[] getFields(){
		return fields;
	}
	
    /**
     * Returns true if record contains a field with a given name.
     * @param name
     * @return
     */
    @Override
	public boolean hasField(String name) {
        return metadata.getField(name) != null;
    }

    /**
     * Returns true if record contains a field with a given label.
     * @param label
     * @return
     */
    @Override
	public boolean hasLabeledField(String label) {
        return metadata.getFieldByLabel(label) != null;
    }
    
	/**
	 *  An attribute that returns metadata object describing the record
	 *
	 * @return    The Metadata value
	 * @since
	 */
	@Override
	public DataRecordMetadata getMetadata() {
		return metadata;
	}


	/**
	 *  An operation that returns numbe of fields this record
     *  contains.
	 *
	 * @return    The NumFields value
	 * @since
	 */
	@Override
	public int getNumFields() {
		return fields.length;
	}


	/**
	 * Initializes the record - creates its fields.
	 * 
	 * <p>This method has to be called before accessing any of the record's fields.
	 *
	 * @since    April 5, 2002
	 */
	@Override
	public void init() {
		if (!isInitialized) {
			DataFieldMetadata fieldMetadata;
			// create appropriate data fields based on metadata supplied
			try {
				for (int i = 0; i < metadata.getNumFields(); i++) {
					fieldMetadata = metadata.getField(i);
					fields[i] = createField(fieldMetadata.getDataType(), fieldMetadata, plain);
				}
			} catch (Exception e) {
				throw new JetelRuntimeException(String.format("Data record '%s' cannot be initialized.", metadata.getName()), e);
			}
			
			isInitialized = true;
		}
	}

	protected DataField createField(DataFieldType fieldType, DataFieldMetadata fieldMetadata, boolean plain) {
		return DataFieldFactory.createDataField(fieldType, fieldMetadata, plain);
	}

	/**
	 *  Serializes this record's content into ByteBuffer.
	 *
	 * @param  buffer  ByteBuffer into which the individual fields of this record should be put
	 * @since          April 23, 2002
	 */
	@Override
	public void serialize(CloverBuffer buffer) {
        for (DataField field : fields) {
        	field.serialize(buffer);
        }
    }

	@Override
	public void serialize(CloverBuffer buffer,DataRecordSerializer serializer) {
		serializer.serialize(buffer, this);
	}
	
	@Override
	public void serialize(CloverBuffer buffer,DataRecordSerializer serializer, int[] whichFields) {
		serializer.serialize(buffer, this, whichFields);
	}
	
	/**
	 * Unitary deserialization should be compatible with
	 * unitary serialization. Moreover this type of unitary
	 * serialization and deserialization should be compatible
	 * with all descendants of DataRecord - for now it is only
	 * Token and DataRecord. So for example serialized
	 * DataRecord can be deserialized into Token and vice versa.
	 */
	@Override
	public void serializeUnitary(CloverBuffer buffer) {
		this.serialize(buffer);
	}
	
	@Override
	public void serializeUnitary(CloverBuffer buffer,DataRecordSerializer serializer) {
		this.serialize(buffer,serializer);
	}

	/**
	 * @deprecated use {@link #serialize(CloverBuffer)} instead
	 */
	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public void serialize(ByteBuffer buffer) {
		CloverBuffer bufferWrapper = CloverBuffer.wrap(buffer);
		serialize(bufferWrapper);
		if (buffer != bufferWrapper.buf()) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
	}

	/**
     * Serializes this record's content into ByteBuffer.<br>
     * Asume only fields which indexes are in fields array
     * 
     * @param buffer
     * @param whichFields
     * @since 27.2.2007
     */
    @Override
	public void serialize(CloverBuffer buffer,int[] whichFields) {
        for(int i:whichFields){
            fields[i].serialize(buffer);
        }
    }

    @Override
	public void serializeUnitary(CloverBuffer buffer,int[] whichFields) {
    	serialize(buffer, whichFields);
    }

    /**
     * @deprecated {@link #serialize(CloverBuffer, int[])} instead
     */
    @SuppressWarnings("deprecation")
	@Override
	@Deprecated
    public void serialize(ByteBuffer buffer, int[] whichFields) {
		CloverBuffer bufferWrapper = CloverBuffer.wrap(buffer);
		serialize(bufferWrapper, whichFields);
		if (buffer != bufferWrapper.buf()) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
    }

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
	@Override
	public void setMetadata(DataRecordMetadata metadata) {
		if (this.metadata != metadata){
		    this.metadata=metadata;
		    fields = new DataField[metadata.getNumFields()];
		}
	}


	/**
	 *  An operation that sets value of all data fields to their default value.
	 */
	@Override
	public void setToDefaultValue() {
		for (int i = 0; i < fields.length; i++) {
			fields[i].setToDefaultValue();
		}
	}


	/**
	 *  An operation that sets value of the selected data field to its default
	 *  value.
	 *
	 * @param  _fieldNum  Ordinal number of the field which should be set to default
	 */
	@Override
	public void setToDefaultValue(int _fieldNum) {
		fields[_fieldNum].setToDefaultValue();
	}

    /**
     *  An operation that sets value of all data fields to NULL value.
     */
    @Override
	public void setToNull(){
        for (int i = 0; i < fields.length; i++) {
            fields[i].setNull(true);
        }
    }
    
    
    /**
     *  An operation that sets value of the selected data field to NULL
     *  value.
     * @param _fieldNum
     */
    @Override
	public void setToNull(int _fieldNum) {
        fields[_fieldNum].setNull(true);
    }
    
    /**
     * An operation which sets/resets all fields to their
     * initial value (just after they were created by JVM) - 
     * it varies depending on type of field.<br>
     * Nullable fields are set to NULL, non-nullable are zeroed, if they
     * have default value defined, then default value is assigned.
     */
    @Override
	public void reset() {
        for (int i = 0; i < fields.length; i++) {
            fields[i].reset();
        }
    }
    
    /**
     * An operation that resets value of specified field - by calling
     * its reset() method.
     * 
     * @param _fieldNum Ordinal number of the field which should be set to default
     */
    @Override
	public void reset(int _fieldNum) {
        fields[_fieldNum].reset();
    }
    
    

	/**
	 *  Creates textual representation of record's content based on values of individual
	 *  fields
	 *
	 * @return    Description of the Return Value
	 */
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer(80);
		for (int i = 0; i < fields.length; i++) {
			str.append("#").append(i).append("|");
			str.append(fields[i].getMetadata().getName()).append("|");
			str.append(fields[i].getMetadata().getDataType().getShortName());
			str.append("->");
			str.append(fields[i].toString());
			str.append("\n");
		}
		return str.toString();
	}


	/**
	 *  Gets the actual size of record (in bytes).<br>
	 *  <i>How many bytes are required for record serialization</i>
	 *
	 * @return    The size value
	 */
	@Override
	public int getSizeSerialized() {
        int size=0;
        for (int i = 0; i < fields.length; size+=fields[i++].getSizeSerialized());
		return size;
	}

	/**
	 * Size of unitary serialization form of this data record.
	 * Unitary deserialization should be compatible with
	 * unitary serialization. Moreover this type of unitary
	 * serialization and deserialization should be compatible
	 * with all descendants of DataRecord - for now it is only
	 * Token and DataRecord. So for example serialized
	 * DataRecord can be deserialized into Token and vice versa.
	 */
	@Override
	public int getSizeSerializedUnitary() {
		return this.getSizeSerialized();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode(){
	    return HashCodeUtil.hash(fields);
	}
    
    /**
     * Test whether the whole record has NULL value - i.e.
     * every field it contains has NULL value.
     * @return true if all fields have NULL value otherwise false
     */
    
    @Override
	public boolean isNull(){
        for (int i = 0; i < fields.length; i++) {
            if (!fields[i].isNull()) return false;
        }
        return true;
    }

    /**
     * Returns iterator over all contained data fields.
     * @see java.lang.Iterable#iterator()
     */
	@Override
	public Iterator<DataField> iterator() {
        return new Iterator<DataField>() {
            private int idx = 0;
            @Override
			public boolean hasNext() {
                return fields.length > idx;
            }

			@Override
			public DataField next() {
                try {
                    return fields[idx++];
                } catch(ArrayIndexOutOfBoundsException e) {
                    throw new NoSuchElementException();
                }
            }

            @Override
			public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

	@Override
	public void setDeserializeAutofilledFields(boolean deserializeAutofilledFields) {
		this.deserializeAutofilledFields = deserializeAutofilledFields;
	}
}
/*
 *  end class DataRecord
 */

