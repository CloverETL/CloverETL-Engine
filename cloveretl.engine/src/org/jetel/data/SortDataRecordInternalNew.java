/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.data;

import java.util.ArrayList;
import java.nio.*;

import org.jetel.metadata.DataRecordMetadata;

/**
 *  Class for simple in-memory sorting of data records.<br>
 *  Incoming records are stored in in-memory buffer(s) and
 *  from each data record, key is extracted and stored in
 *  ArrayList.<br>
 *  When storing phase is finished, sort method sorts the
 *  records in ascending order(java.util.Arrays.sort method is used).<br>
 *  Last phase (
 *
 *@author     dpavlis
 *@see	      RecordKey
 */
public class SortDataRecordInternalNew {

	private DataRecord[] recordArray;
	private ArrayList keyList;
	private ByteBuffer dataBuffer;
	private RecordKey key;
	private DataRecordMetadata metadata;
	private int recCounter;
	private int readPos;
	private boolean sortOrderAscending;

	private final static int KEYLIST_INITIAL_CAPACITY = 99999;

	/**
	 *  Constructor for the DataRecordSort object
	 *
	 *@param  metadata       Description of the Parameter
	 *@param  keyItems       Description of the Parameter
	 *@param  sortAscending  Description of the Parameter
	 */
	public SortDataRecordInternalNew(DataRecordMetadata metadata, String[] keyItems, boolean sortAscending) {
		this.metadata = metadata;
		keyList = new ArrayList(KEYLIST_INITIAL_CAPACITY);
		// allocate buffer for storing values
		dataBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		key = new RecordKey(keyItems, metadata);
		key.init();
		sortOrderAscending = sortAscending;
		recCounter = 0;
		readPos=0;
	}


	/**
	 *  Resets all counters and empties internal buffers. The sorting (feeding) can
	 *  be then restarted
	 */
	public void reset() {
		recCounter = 0;
		keyList.clear();
		dataBuffer.clear();
	}


	/**
	 *  Rewinds the record pointer so the records can be read again
	 */
	public void rewind() {
		readPos = 0;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  record  Description of the Parameter
	 */
	public void put(DataRecord record) {
		dataBuffer.clear();
		record.serialize(dataBuffer);
		dataBuffer.flip();
		DataRecord newRecord=new DataRecord(metadata);
		newRecord.init();
		newRecord.deserialize(dataBuffer);
		keyList.add(newRecord);
		recCounter++;
	}

	public void put(ByteBuffer record){
		DataRecord newRecord=new DataRecord(metadata);
		newRecord.init();
		newRecord.deserialize(record);
		keyList.add(newRecord);
		recCounter++;
	}

	/**
	 *  Sorts internal array containing individual records' keys
	 */
	public void sort() {
		// ?? would it improve anything ?? -> keyList.trimToSize();
		recordArray = (DataRecord[]) keyList.toArray(new DataRecord[1]);
		RecordComparator comparator=new RecordComparator(key.getKeyFields());
		
		java.util.Arrays.sort(recordArray, 0, recCounter,comparator);
		if (sortOrderAscending) {
			readPos = 0;
		} else {
			readPos = recCounter - 1;
		}
	}


	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  record  DataRecord which populate with data
	 *@return         The next record or null if no more records
	 */
	public DataRecord getNext(DataRecord record) {
		if ((sortOrderAscending && (readPos >= recCounter)) || (readPos < 0)) {
			return null;
		}
		dataBuffer.clear();
		recordArray[readPos].serialize(dataBuffer);
		dataBuffer.flip();
		record.deserialize(dataBuffer);
		if (sortOrderAscending) {
			readPos++;
		} else {
			readPos--;
		}
		return record;
	}


	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  recordData  ByteBuffer into which copy next record's data
	 *@return             The next record or null if no more records
	 */
	public boolean getNext(ByteBuffer recordData) {
		if ((sortOrderAscending && (readPos >= recCounter)) || (readPos < 0)) {
			return false;
		}
		dataBuffer.clear();
		recordArray[readPos].serialize(dataBuffer);
		dataBuffer.flip();
		recordData.put(dataBuffer);
		recordData.flip();
		
		if (sortOrderAscending) {
			readPos++;
		} else {
			readPos--;
		}
		return true;
	}


	/**
	 *  Description of the Method
	 */
	public void dumpRecArray() {
		for (int i = 0; i < recCounter; i++) {
			System.out.print("#" + i);
			System.out.println(recordArray[i]);
		}
	}

}

