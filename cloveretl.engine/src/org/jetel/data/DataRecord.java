/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
// FILE: c:/projects/jetel/org/jetel/data/DataRecord.java

package org.jetel.data;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.BitArray;

/**
 *  A class that represents one data record with structure based on provided
 *  metadata object.<br> 
 *  Fields of this data record are created only once and updated when it is needed.<br>
 *  When we need to send record through the EDGE (or other communication channel), we just serialize it 
 *  to stream of bytes (it isn't standard Java serializing, but Clover's own).
 *
 * @author      D.Pavlis
 * @since       March 26, 2002
 * @revision    $Revision$
 * @created     May 18, 2003
 * @see         org.jetel.metadata.DataRecordMetadata
 */
public class DataRecord implements Serializable, Comparable {

    private static ThreadLocal<byte[]> NULLs_BYTE_BUFFER = new ThreadLocal<byte[]>();
    private static final boolean HANDLE_NULLABLE = false;
    
	/**
     * Currently not used
	 * @since
	 */
	// private transient String codeClassName;

	/**
     * Array for holding data fields
     * 
	 * @since
	 */
	private DataField fields[];

	/**
     * Reference to metadata object describing this record 
	 * @since
	 */
	private transient DataRecordMetadata metadata;

    
    /**
     * Should this record and all its fields be created in plain mode ?<br>
     * Plai means no "decorators" will be added and this record is deemed to
     * store values only, no formating or parsing will be performed on this
     * record.
     * 
     */
    private boolean plain=false;
    
	/**
	 * Create new instance of DataRecord based on specified metadata (
	 * how many fields, what field types, etc.)
	 * 
	 * @param _metadata  description of the record structure
	 */
	public DataRecord(DataRecordMetadata _metadata) {
		this(_metadata,false);
	}
    
    /**
     * Create new instance of DataRecord based on specified metadata (
     * how many fields, what field types, etc.)
     * 
     * @param _metadata description of the record structure
     * @param plain if true, no formatters and other "extra" objects will be created for
     * fields
     */
    public DataRecord(DataRecordMetadata _metadata,boolean plain ) {
        this.metadata = _metadata;
        this.plain=plain;
        fields = new DataField[metadata.getNumFields()];
    }

	/**
	 * Private constructor used when clonning/copying DataRecord objects.<br>
     * It takes numFields parameter to speed-up creation.
	 * 
	 * @param _metadata metadata describing this record
	 * @param numFields number of fields this record should contain
	 */
	private DataRecord(DataRecordMetadata _metadata, int numFields){
	    this.metadata = _metadata;
	    fields = new DataField[numFields];
	}

	
	
	/**
	 * Creates deep copy of existing record (field by field).
	 * 
	 * @return new DataRecord
	 */
	public DataRecord duplicate(){
	    DataRecord newRec=new DataRecord(metadata,fields.length);
	    for (int i=0;i<fields.length;i++){
	        newRec.fields[i]=fields[i].duplicate();
	    }
	    return newRec;
	}
	
	/**
	 * Set fields by copying the fields from the record passed as argument.
	 * Does assume that both records have the same structure - i.e. metadata.
	 * @param fromRecord DataRecord from which to get fields' values
	 */
	public void copyFrom(DataRecord fromRecord){
	    for (int i=0;i<fields.length;i++){
	        this.fields[i].copyFrom(fromRecord.fields[i]);
	    } 
	}
	
	
	/**
	 *  Set fields by copying the fields from the record passed as argument.
	 * Can handle situation when records are not exactly the same.
	 *
	 * @param  _record  Record from which fields are copied
	 * @since
	 */

	public void copyFieldsByPosition(DataRecord _record) {
		DataRecordMetadata sourceMetadata = _record.getMetadata();
		DataField sourceField;
		DataField targetField;
		//DataFieldMetadata fieldMetadata;
		int sourceLength = sourceMetadata.getNumFields();
		int targetLength = this.metadata.getNumFields();
		int copyLength;
		if (sourceLength < targetLength) {
			copyLength = sourceLength;
		} else {
			copyLength = targetLength;
		}

		for (int i = 0; i < copyLength; i++) {

			//fieldMetadata = metadata.getField(i);
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
	 *  Set fields by copying name-matching fields' values from the record passed as argument.<br>
     *  If two fields match by name but not by type, target field is set to default value.
	 *  
	 * @param sourceRecord from which copy data
	 * @return boolean array with true values on positions, where values were copied from source record
	 */
	public boolean[] copyFieldsByName(DataRecord sourceRecord) {
        boolean[] result = new boolean[fields.length];
        Arrays.fill(result, false);
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
                if (sourceField.getType() == targetField.getType()) {
                    targetField.setValue(sourceField.getValue());
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
	public void delField(int _fieldNum) {
            DataField tmp_fields[]=new DataField[fields.length-1];
            DataRecordMetadata tmp_metadata=new DataRecordMetadata(metadata.getName(),metadata.getRecType());
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
	public void deserialize(ByteBuffer buffer) {
        if (HANDLE_NULLABLE && metadata.isNullable()) {
          BitArray bits=new BitArray(fields.length);
          bits.deserialize(buffer);
          for (int i=0;i<fields.length;i++){
              if (bits.isSet(i)){
                  fields[i].setNull(true);
              }else{
                  fields[i].deserialize(buffer);
              }
          }
          
        } else {

            for (int i = 0; i < fields.length; i++) {
                fields[i].deserialize(buffer);
            }
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
	public int compareTo(Object obj){
	    if (this==obj) return 0;
	    
	    if (obj instanceof DataRecord) {
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
	 *  Gets the codeClassName attribute of the DataRecord object
	 *
	 * @return    The codeClassName value
	 */
	public String getCodeClassName() {
		return ""; //codeClassName;
	}


	/**
	 *  An operation that returns DataField with
     *  specified order number.
	 *
	 * @param  _fieldNum  Description of Parameter
	 * @return            The Field value
	 * @since
	 */
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
	public DataField getField(String _name) {
	    return fields[metadata.getFieldPosition(_name)];
	}

    /**
     * Returns true if record contains a field with a given name.
     * @param name
     * @return
     */
    public boolean hasField(String name) {
        return metadata.getField(name) != null;
    }

	/**
	 *  An attribute that returns metadata object describing the record
	 *
	 * @return    The Metadata value
	 * @since
	 */
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
	public int getNumFields() {
		return fields.length;
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
					fieldMetadata,plain);
		}
	}


	/**
	 *  Serializes this record's content into ByteBuffer.
	 *
	 * @param  buffer  ByteBuffer into which the individual fields of this record should be put
	 * @since          April 23, 2002
	 */
	public void serialize(ByteBuffer buffer) {
        if (HANDLE_NULLABLE && metadata.isNullable()) {
           BitArray bits=new BitArray(fields.length);
           buffer.mark();
           bits.serialize(buffer);
           for(int i=0;i<fields.length;i++){
               if (fields[i].isNull){
                   bits.set(i);
               }else{
                   fields[i].serialize(buffer);
               }
           }
           final int pos=buffer.position();
           buffer.reset();
           bits.serialize(buffer);
           buffer.position(pos);
           
        } else {
            for (int i = 0; i < fields.length; i++) {
                fields[i].serialize(buffer);
            }
        }
    }


	/**
	 *  Sets the codeClassName attribute of the DataRecord object
	 *
	 * @param  codeClassName  The new codeClassName value
	 */
	public void setCodeClassName(String codeClassName) {
		// this.codeClassName = codeClassName;
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
	public void setMetadata(DataRecordMetadata metadata) {
		if (this.metadata != metadata){
		    this.metadata=metadata;
		    fields = new DataField[metadata.getNumFields()];
		}
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
	 * @param  _fieldNum  Ordinal number of the field which should be set to default
	 */
	public void setToDefaultValue(int _fieldNum) {
		fields[_fieldNum].setToDefaultValue();
	}

    /**
     *  An operation that sets value of all data fields to NULL value.
     */
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
    public void reset(int _fieldNum) {
        fields[_fieldNum].reset();
    }
    
    

	/**
	 *  Creates textual representation of record's content based on values of individual
	 *  fields
	 *
	 * @return    Description of the Return Value
	 */
	public String toString() {
		StringBuffer str = new StringBuffer(80);
		for (int i = 0; i < fields.length; i++) {
			str.append("#").append(i).append("|");
			str.append(fields[i].getMetadata().getName()).append("|");
			str.append(fields[i].getType());
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
	public int getSizeSerialized() {
        System.err.println("getSize");
        int size=0;
        int inNull=0;
        if (metadata.isNullable()){
            for (int i = 0; i < fields.length;i++){
                if (fields[i].isNull()){
                    inNull++;
                }else{
                    size+=fields[i].getSizeSerialized(); 
                }
            }
            size+=BitArray.bitsLength2Bytes(metadata.getNumNullableFields());
        }else{
            for (int i = 0; i < fields.length; size+=fields[i++].getSizeSerialized());
        }
		return size;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode(){
	    int hash=17;
	    for (int i=0;i<fields.length;i++){
	        hash=37*hash+fields[i].hashCode();
	    }
	    return hash;
	}
    
    /**
     * Test whether the whole record has NULL value - i.e.
     * every field it contains has NULL value.
     * @return true if all fields have NULL value otherwise false
     */
    
    public boolean isNull(){
        for (int i = 0; i < fields.length; i++) {
            if (!fields[i].isNull()) return false;
        }
        return true;
    }
    
}
/*
 *  end class DataRecord
 */

