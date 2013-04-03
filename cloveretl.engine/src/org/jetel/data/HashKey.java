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

/**
 *  Class associated with RecorKey and DataRecord
 *  Intended primarily to be used as a key for HashTable
 *  It only points to RecordKey and particular data record
 *  so we can perform hashing & lookup of the data record.
 *
 * @author      DPavlis
 * @since       6. March 2004
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
	@Override
	public int hashCode() {
		int hash = 17;
		int[] keyFields = recKey.getKeyFields();
		for (int i = 0; i < keyFields.length; i++) {
			hash = 37*hash + record.getField(keyFields[i]).hashCode();
		}
		return hash;
	}

	/**
	 * Checks whether this <code>HashKey</code> equals to the given object. This happens only if the given object
	 * is also a <code>HashKey</code>, it has the same number of key fields and these are all equal. The
	 * {@link RecordKey#compare(RecordKey, DataRecord, DataRecord)} method is used for comparison of the key fields.
	 *
	 * @param object an object to be compared to this instance
	 *
	 * @return <code>true</code> if the given object is equal to this instance, <code>false</code> otherwise
	 *
	 * @see Object#equals(Object)
	 */
	@Override
	public boolean equals(Object object) {
        if (object == this) {
            return true;
        }

        if (!(object instanceof HashKey)) {
            return false;
        }

        HashKey hashKey = (HashKey) object;

        return recKey.equals(hashKey.getRecordKey(), record, hashKey.getDataRecord());
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
	
	@Override
	public String toString(){
		StringBuffer strBuf=new StringBuffer();
		int[] keyFields = recKey.getKeyFields();
		for (int i = 0; i < keyFields.length; i++) {
			strBuf.append("$").append(record.getField(keyFields[i]).getValue()).append("$");
		}
		return strBuf.toString();
	}
}

