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

/**
 *  Class associated with RecorKey and DataRecord
 *  Intended primarily to be used as a key for HashTable
 *  It only points to RecordKey and particular data record
 *  so we can perform hashing & lookup of the data record.
 *
 * @author      DPavlis
 * @since       6. March 2004
 * @revision    $Revision$
 */
public class HashKey {
	private RecordKey recKey;
	private DataRecord record;


	/**
	 *  Description of the Method
	 *
	 * @param  recKey  Description of the Parameter
	 * @param  record  Description of the Parameter
	 */
	public HashKey(RecordKey recKey, DataRecord record) {
		this.recKey = recKey;
		this.record = record;
	}


	/**
	 *  Calculates hashCode for hashkey - based on
	 * fields values which compose hashkey
	 *
	 * @return    Description of the Return Value
	 */
	public int hashCode() {
		int hash = 17;
		int[] keyFields = recKey.getKeyFields();
		for (int i = 0; i < keyFields.length; i++) {
			hash = 37*hash + record.getField(keyFields[i]).hashCode();
		}
		return hash;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  otherObject  Description of the Parameter
	 * @return      Description of the Return Value
	 */
	public boolean equals(Object otherObject) {
		if( otherObject == null || !(otherObject.getClass().equals(this.getClass()))){
			return false;
		}
		final HashKey other = (HashKey) otherObject;
		
		int[] keyFields = recKey.getKeyFields();
		DataRecord record2 = other.getDataRecord();
		int key2Fields[] = other.getKeyFields();
		
		if (keyFields.length != key2Fields.length) {
			return false;
			//throw RuntimeException();
			// we should throw exception as we are going to compare
			// keys with different number of fields
		}
		// if field types are different for two key fields, we evaluate it as FALSE - not equal
		// however it should probably raise an exception ...
		
		for (int i = 0; i < keyFields.length; i++) {
			if (!record.getField(keyFields[i]).equals(record2.getField(key2Fields[i]))) {
				return false;
			}
		}
		return true;
	}


	/**
	 *  Gets the recordKey attribute of the HashKey object
	 *
	 * @return    The recordKey value
	 */
	public RecordKey getRecordKey() {
		return recKey;
	}


	/**
	 *  Gets the dataRecord attribute of the HashKey object
	 *
	 * @return    The dataRecord value
	 */
	public DataRecord getDataRecord() {
		return record;
	}


	/**
	 *  Gets the keyFields attribute of the HashKey object
	 *
	 * @return    The keyFields value
	 */
	public int[] getKeyFields() {
		return recKey.getKeyFields();
	}


	/**
	 *  Sets the dataRecord attribute of the HashKey object
	 *
	 * @param  record  The new dataRecord value
	 */
	public void setDataRecord(DataRecord record) {
		this.record = record;
	}


	/**
	 *  Sets the recordKey attribute of the HashKey object
	 *
	 * @param  key  The new recordKey value
	 */
	public void setRecordKey(RecordKey key) {
		this.recKey = key;
	}
	
	public String toString(){
		StringBuffer strBuf=new StringBuffer();
		int[] keyFields = recKey.getKeyFields();
		for (int i = 0; i < keyFields.length; i++) {
			strBuf.append("$").append(record.getField(keyFields[i]).getValue()).append("$");
		}
		return strBuf.toString();
	}
}

