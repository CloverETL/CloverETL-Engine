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
public class SortDataRecordInternal {

	private KeyItem[] keyArray;
	private ArrayList keyList;
	private ByteBuffer[] dataBuffer;
	private RecordKey key;
	private DataRecordMetadata metadata;
	private int recCounter;
	private int readPos;
	private short bufferCounter;
	private boolean sortOrderAscending;

	private final static int KEYLIST_INITIAL_CAPACITY = 99999;

	/**
	 *  Constructor for the DataRecordSort object
	 *
	 *@param  metadata       Description of the Parameter
	 *@param  keyItems       Description of the Parameter
	 *@param  sortAscending  Description of the Parameter
	 */
	public SortDataRecordInternal(DataRecordMetadata metadata, String[] keyItems, boolean sortAscending) {
		this.metadata = metadata;
		keyList = new ArrayList(KEYLIST_INITIAL_CAPACITY);
		dataBuffer = new ByteBuffer[Defaults.Data.MAX_BUFFERS_ALLOCATED];
		// allocate initial buffer (at least one)
		dataBuffer[0] = ByteBuffer.allocateDirect(Defaults.Data.DATA_RECORDS_BUFFER_SIZE);
		key = new RecordKey(keyItems, metadata);
		key.init();
		sortOrderAscending = sortAscending;
		recCounter = 0;
		bufferCounter = 0;
	}


	/**
	 *  Resets all counters and empties internal buffers. The sorting (feeding) can
	 *  be then restarted
	 */
	public void reset() {
		recCounter = 0;
		readPos = 0;
		bufferCounter = 0;
		keyList.clear();
		dataBuffer = new ByteBuffer[Defaults.Data.MAX_BUFFERS_ALLOCATED];
		dataBuffer[0] = ByteBuffer.allocateDirect(Defaults.Data.DATA_RECORDS_BUFFER_SIZE);
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
		int offset = 0;
		int length = 0;
		boolean overflow = false;
		String keyStr;
		if ((keyStr = key.getKeyString(record)) == null) {
			throw new RuntimeException("Null KEY generated!");
		}
		try {
			offset = dataBuffer[bufferCounter].position();
			record.serialize(dataBuffer[bufferCounter]);
			length = dataBuffer[bufferCounter].position() - offset;
		} catch (BufferOverflowException ex) {
			// buffer overflow -> add one more buffer if it is allowed
			if (bufferCounter < Defaults.Data.MAX_BUFFERS_ALLOCATED) {
				overflow = true;
			} else {
				throw new BufferOverflowException();
			}
		}
		if (overflow) {
			bufferCounter++;
			dataBuffer[bufferCounter] = ByteBuffer.allocateDirect(Defaults.Data.DATA_RECORDS_BUFFER_SIZE);
			offset = dataBuffer[bufferCounter].position();
			record.serialize(dataBuffer[bufferCounter]);
			length = dataBuffer[bufferCounter].position() - offset;
		}
		keyList.add(new KeyItem(keyStr, offset, length, bufferCounter));
		recCounter++;
	}


	/**
	 *  Sorts internal array containing individual records' keys
	 */
	public void sort() {
		// ?? would it improve anything ?? -> keyList.trimToSize();
		keyArray = (KeyItem[]) keyList.toArray(new KeyItem[1]);
		java.util.Arrays.sort(keyArray, 0, recCounter);
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
		short whichBuffer = keyArray[readPos].getBuffer();
		// set new position & limit in DATA BUFFER
		dataBuffer[whichBuffer].position(keyArray[readPos].getRecordOffset());
		record.deserialize(dataBuffer[whichBuffer]);
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
		int oldLimit;
		if ((sortOrderAscending && (readPos >= recCounter)) || (readPos < 0)) {
			return false;
		}
		short whichBuffer = keyArray[readPos].getBuffer();
		//let's preserve the limit
		oldLimit = dataBuffer[whichBuffer].limit();
		// set new position & limit in DATA BUFFER
		dataBuffer[whichBuffer].position(keyArray[readPos].getRecordOffset());
		dataBuffer[whichBuffer].limit((keyArray[readPos].getRecordOffset() + keyArray[readPos].getRecordLength()));
		// copy the record's data
		recordData.put(dataBuffer[whichBuffer]);
		dataBuffer[whichBuffer].limit(oldLimit);
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
	private void dumpKeyArray() {
		for (int i = 0; i < recCounter; i++) {
			System.out.print("#" + i);
			System.out.println(" Key [" + keyArray[i].getKey() + " B: " + keyArray[i].getBuffer() + "] offset: " 
			+ keyArray[i].getRecordOffset() + " length: " +
			keyArray[i].getRecordLength() + " Noffset " + 
			(keyArray[i].getRecordOffset() + keyArray[i].getRecordLength()));
		}
	}


	/**
	 *  Description of the Class
	 *
	 *@author     dpavlis
	 */
	class KeyItem implements Comparable {
		String key;
		int recordOffset;
		int recordLength;
		short bufferNo;


		/**
		 *  Constructor for the KeyItem object
		 *
		 *@param  key       Description of the Parameter
		 *@param  offset    Description of the Parameter
		 *@param  length    Description of the Parameter
		 *@param  bufferNo  Description of the Parameter
		 */
		KeyItem(String key, int offset, int length, short bufferNo) {
			this.key = key;
			this.recordOffset = offset;
			this.recordLength = length;
			this.bufferNo = bufferNo;
		}


		/**
		 *  Gets the key attribute of the KeyItem object
		 *
		 *@return    The key value
		 */
		String getKey() {
			return key;
		}


		/**
		 *  Gets the recordOffset attribute of the KeyItem object
		 *
		 *@return    The recordOffset value
		 */
		int getRecordOffset() {
			return recordOffset;
		}


		/**
		 *  Gets the recordLength attribute of the KeyItem object
		 *
		 *@return    The recordLength value
		 */
		int getRecordLength() {
			return recordLength;
		}


		/**
		 *  Gets the buffer attribute of the KeyItem object
		 *
		 *@return    The buffer value
		 */
		short getBuffer() {
			return bufferNo;
		}


		/**
		 *  Description of the Method
		 *
		 *@param  secondKey  Description of the Parameter
		 *@return            Description of the Return Value
		 */
		public int compareTo(Object secondKey) {
			return key.compareTo(((KeyItem) secondKey).getKey());
		}
	}

}

