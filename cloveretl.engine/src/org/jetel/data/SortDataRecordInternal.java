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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.jetel.metadata.DataRecordMetadata;

/**
 *  Class for simple in-memory sorting of data records.<br>
 *  Incoming records are stored in in-memory buffer(s).
 *  Buffers are allocated on the fly with increasing capacity
 *  up to defined limit.<br>
 *  When storing phase is finished, sort method sorts the
 *  records in ascending order(java.util.Arrays.sort method is used).<br>
 *  After sorting is finished, reading can start. If descending order
 *  is specified, the records returned by get() method are in descending order.<br>
 *  Standard way of working with SortDataRecordInternal:<br>
 *  <ol>
 *  <li>put() n-times
 *  <li>sort()
 *  <li>get() n-times
 *  </ol>
 *@author     dpavlis
 *@see	      org.jetel.data.RecordKey
 */
public class SortDataRecordInternal {

	private DataRecordCol currentRecordCol;
	private int currentColSize;
	private int currentColIndex;
	private ByteBuffer dataBuffer;
	private RecordKey key;
	private DataRecordMetadata metadata;
	private int recCounter;
	private int lastFound;
	private boolean sortOrderAscending;
	private List recordColList;
	private DataRecordCol[] recordColArray;

	private final static int DEFAULT_RECORD_COLLECTION_CAPACITY = 2000;
	private final static int DEFAULT_MAX_NUM_COLLECTIONS = 8;

	/**
	 * Comment for <code>COLLECTION_GROW_FACTOR</code>
	 * Used for growing collection capacity when adding 2nd and additional collections
	 * (the actual value is multiplied by 10)
	 */
	private final static int COLLECTION_GROW_FACTOR = 16; 


	/**
	 * Constructor for the SortDataRecordInternal
	 * 
	 * @param metadata	Metadata describing records stored in internal buffer
	 * @param keyItems	Names of fields which compose the key used for sorting data
	 * @param sortAscending	True if required sort order is Ascending, otherwise False
	 * @param oneColCapacity	What is the initial capacity of 1 chunk/array of data records
	 */
	public SortDataRecordInternal(DataRecordMetadata metadata, String[] keyItems, boolean sortAscending, int oneColCapacity) {
		this.metadata = metadata;
		recordColList = new ArrayList(DEFAULT_MAX_NUM_COLLECTIONS);
		// allocate buffer for storing values
		dataBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		key = new RecordKey(keyItems, metadata);
		key.init();
		sortOrderAscending = sortAscending;
		recCounter=lastFound=currentColIndex= 0;
		currentRecordCol=new DataRecordCol(oneColCapacity,metadata,sortOrderAscending);
		currentColSize=oneColCapacity;
		recordColList.add(currentRecordCol);
	}


	/**
	 * @see SortDataRecordInternal
	 * 
	 */
	public SortDataRecordInternal(DataRecordMetadata metadata, String[] keyItems, boolean sortAscending){
	    this(metadata,keyItems,sortAscending,DEFAULT_RECORD_COLLECTION_CAPACITY);
	}
	
	public void setInitialBufferCapacity(int capacity){
	    //if (capacity>100){
	        currentColSize=capacity;
	    //}
	}
	
	/**
	 *  Resets all counters and empties internal buffers. The sorting (feeding) can
	 *  then be restarted. Existing pre-allocated buffers/collections will
	 *  be reused.
	 */
	public void reset() {
		recCounter = 0;
		lastFound=0;
		dataBuffer.clear();
		currentColIndex=0;
		currentRecordCol=(DataRecordCol)recordColList.get(0);
		for(Iterator i=recordColList.iterator();i.hasNext();){
		    ((DataRecordCol)i.next()).reset();
		}
	}

	/**
	 * Frees all resources (buffers, collections, etc)
	 */
	public void free(){
	    for(Iterator i=recordColList.iterator();i.hasNext();){
		    ((DataRecordCol)i.next()).free();
		}
	    recordColList.clear();
	}

	/**
	 *  Rewinds the record pointer so the sorted records can be read again.
	 */
	public void rewind() {
		lastFound = 0;
		for(Iterator i=recordColList.iterator();i.hasNext();){
		    ((DataRecordCol)i.next()).rewind();
		}
	}


	/**
	 *  Stores additional record into internal buffer for sorting
	 *
	 *@param  record  DataRecord to be stored
	 */
	public boolean put(DataRecord record) {
		if (!currentRecordCol.put(record)){
		    if (!secureSpace()){
		        return false;
		    }
		    if (!currentRecordCol.put(record)){
		        throw new RuntimeException("Assertion problem: Can't store DataRecords in newly allocated DataRecordCol");
		    }
		}
		recCounter++;
		return true;
	}

	/**
	 * Stores additional record into internal buffer for sorting
	 * 
	 * @param record
	 */
//	public boolean put(ByteBuffer record){
//		if (!currentRecordCol.put(record)){
//		    if (!secureSpace()){
//		        return false;
//		    }
//		    if (!currentRecordCol.put(record)){
//		        throw new RuntimeException("Assertion problem: Can't store DataRecords in newly allocated DataRecordCol");
//		    }
//		}
//		recCounter++;
//		return true;
//	}

	private final boolean secureSpace(){
	    if (currentColIndex+1>=DEFAULT_MAX_NUM_COLLECTIONS){
	        return false;
	    }
	    /* add record collection (if it doesn't exist already)
	     * if it exists, just use it (we assume that it hase been reseted in such case) 
	    */  
	    currentColIndex++;
	    if (currentColIndex>=recordColList.size()){
	        currentColSize=(currentColSize*COLLECTION_GROW_FACTOR)/10;
	        currentRecordCol=new DataRecordCol(currentColSize,metadata,sortOrderAscending);
	        recordColList.add(currentRecordCol);
	    }else{
	        currentRecordCol=(DataRecordCol)recordColList.get(currentColIndex);
	    }
	    return true;
	}
	
	/**
	 *  Sorts internal array containing individual records
	 */
	public void sort() {
	    RecordComparator comparator=new RecordComparator(key.getKeyFields());
	    DataRecordCol recordArray;
	    for (Iterator iterator=recordColList.iterator();iterator.hasNext();){
	        recordArray=((DataRecordCol)iterator.next());
	        // sort it now
	        java.util.Arrays.sort(recordArray.getRecordArray(), 0, recordArray.noItems,comparator);
	        recordArray.rewind(); // rewind to position reader pointer properly (first/last depending on sort order)
	    }
	    // for faster access, convert list to array
	    recordColArray=(DataRecordCol[])recordColList.toArray(new DataRecordCol[0]);
	}


	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  record  DataRecord which populate with data
	 *@return         The next record or null if no more records
	 */
	public DataRecord get() {
	    // optimization - if only 1 sorted buffer, then no merge sorting
	    if (recordColArray.length==1){
	        return recordColArray[0].get();
	    }
	    if (sortOrderAscending){
	        return getNextLowest();
	    }else{
	        return getNextHighest();
	    }
	}


	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  recordData  ByteBuffer into which copy next record's data
	 *@return             True if there was next record or False
	 */
	public boolean get(ByteBuffer recordDataBuffer) {
		DataRecord record=get();
		if (record!=null){
		    record.serialize(recordDataBuffer);
		    recordDataBuffer.flip();
		    return true;
		}else{
		    return false;
		}
	}

	/**
	 * Merge all sorted buffers
	 * 
	 * @return next lowest Data Record based on specified key
	*/
	private DataRecord getNextLowest(){
	    DataRecord record=null,recordNext=null;
	    int index,indexLowest;
	    for(index=lastFound;index<recordColArray.length;index++){
	        record=recordColArray[index].peek();
	        if (record!=null) break;
	    }
	    if (record==null) return null;
	    indexLowest=lastFound=index;
	    for(int i=index+1;i<recordColArray.length;i++){
	        recordNext=recordColArray[i].peek();
	        if ((recordNext!=null)&&(key.compare(record,recordNext)>0)){
	        	record=recordNext;
	        	indexLowest=i;
	        }
	    }
	    return recordColArray[indexLowest].get();
	}
	
	/**
	 * Merge all sorted buffers
	 * 
	 * @return next highest Data Record based on specified key
	 */
	private DataRecord getNextHighest(){
	    DataRecord record=null,recordNext=null;
	    int index,indexHighest;
	    for(index=lastFound;index<recordColArray.length;index++){
	        record=recordColArray[index].peek();
	        if (record!=null) break;
	    }
	    if (record==null) return null;
	    indexHighest=lastFound=index;
	    for(int i=index+1;i<recordColArray.length;i++){
	        recordNext=recordColArray[i].peek();
	        if ((recordNext!=null)&&(key.compare(record,recordNext)<0)){
	        	record=recordNext;
	        	indexHighest=i;
	        }
	    }
	    return recordColArray[indexHighest].get();
	}
	
	  /**
     * @return Returns the recCounter.
     */
    public int getRecCounter() {
        return recCounter;
    }
	
	/**
	 * Helper class which stores array of data records.
	 * rewind() should be called after storing part is completed
	 * 
	 * @author david
	 * @since  13.1.2005
	 *
	 * 
	 */
	private static class DataRecordCol{
		DataRecord recordArray[];
		DataRecordMetadata metadata;
		boolean ascendingOrder;
		int pointer;
		int noItems;
		
		
		DataRecordCol(int capacity,DataRecordMetadata metadata,boolean ascendingOrder){
			recordArray=new DataRecord[capacity];
			noItems=0;
			pointer=0;
			this.metadata=metadata;
			this.ascendingOrder=ascendingOrder;
		}
		
		DataRecord[] getRecordArray(){
		    return recordArray;
		}
		
		boolean put(DataRecord record){
			if (noItems<recordArray.length){
			    if (recordArray[noItems]==null){
			        recordArray[noItems]=record.duplicate();
			    }else{
			        recordArray[noItems].copyFrom(record);
			    }
			    noItems++;
				return true;
			}else{
				return false;
			}
		}
	
//		boolean put(ByteBuffer serializedRecord){
//			if (noItems<recordArray.length){
//				if (recordArray[noItems]==null){
//					recordArray[noItems]=new DataRecord(metadata);
//					recordArray[noItems].init();
//				}
//				recordArray[noItems].deserialize(serializedRecord);
//				noItems++;
//				return true;
//			}else{
//				return false;
//			}
//		}
		
		DataRecord get(){
		    if (ascendingOrder){
		        if (pointer<noItems){
		            return recordArray[pointer++];
		        }else{
		            return null;
		        }
		    }else{
		        if (pointer>=0){
		            return recordArray[pointer--];
		        }else{
		            return null;
		        }
		    }
		}
		
		/**
		 * Returns DataRecord at current pointer position without advancing to
		 * the next.
		 * 
		 * @return DataRecord or Null if no more data
		 */
		DataRecord peek(){
		    if ((pointer<noItems)&&(pointer>=0)){
		        return recordArray[pointer];
		    }else{
		        return null;
		    }
		}
		void rewind(){
		    if (ascendingOrder){
		        pointer=0;
		    }else{
		        pointer=noItems-1;
		    }
		}
		
		void reset(){
			pointer=noItems=0;
		}
		
		void free(){
			reset();
			Arrays.fill(recordArray,null);
		}
		
		void dumpArray() {
			for (int i = 0; i < noItems; i++) {
				System.out.print("#" + i +":");
				System.out.println(recordArray[i]);
			}
		}
		
	}

  
}

