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
import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.CloverBuffer;

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
public class InternalSortDataRecord implements ISortDataRecord {

	private DataRecordCol currentRecordCol;
	private int currentColSize;
	private int currentColIndex;
	private RecordOrderedKey key;
	private DataRecordMetadata metadata;
	private int recCounter;
	private int lastFound;
	private boolean[] sortOrderings; 
	private List<DataRecordCol> recordColList;
	private DataRecordCol[] recordColArray;
	private int numCollections;
    private boolean useCollator=false;
    private RuleBasedCollator collator;
    private RecordOrderedComparator comparator;

	private final static int DEFAULT_NUM_COLLECTIONS = 8;

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
	public InternalSortDataRecord(DataRecordMetadata metadata, String[] keyItems, boolean[] sortOrderings, boolean growingBuffer, int oneColCapacity) {
		this.metadata = metadata;
        this.numCollections = growingBuffer ? DEFAULT_NUM_COLLECTIONS : 1;
        this.sortOrderings = sortOrderings;
		recordColList = new ArrayList<DataRecordCol>(this.numCollections);
		key = new RecordOrderedKey(keyItems, sortOrderings, metadata);
		key.setEqualNULLs(true);
		key.init();
		recCounter=lastFound=currentColIndex= 0;
		currentRecordCol=new DataRecordCol(oneColCapacity);
		currentColSize=oneColCapacity;
		recordColList.add(currentRecordCol);
		updateUseCollatorIndicator(metadata, keyItems);
	}

    public InternalSortDataRecord(DataRecordMetadata metadata, String[] keyItems, boolean[] sortOrderings, boolean growingBuffer) {
    	this(metadata,keyItems, sortOrderings, growingBuffer, Defaults.InternalSortDataRecord.DEFAULT_INTERNAL_SORT_BUFFER_CAPACITY);
    }

	public InternalSortDataRecord(DataRecordMetadata metadata, String[] keyItems, boolean[] sortOrderings){
		this(metadata,keyItems, sortOrderings, true, Defaults.InternalSortDataRecord.DEFAULT_INTERNAL_SORT_BUFFER_CAPACITY);
	}
	
	public void setInitialBufferCapacity(int capacity){
	    currentColSize = Math.max(10, capacity);
	}
	
	/**
	 *  Resets all counters and empties internal buffers. The sorting (feeding) can
	 *  then be restarted. Existing pre-allocated buffers/collections will
	 *  be reused.
	 */
	@Override
	public void reset() {
		recCounter = 0;
		lastFound=0;
		//dataBuffer.clear();
		currentColIndex=0;
		if (recordColList.size()>0) {
			currentRecordCol=(DataRecordCol)recordColList.get(0);
			for (Iterator<DataRecordCol> i = recordColList.iterator(); i.hasNext();) {
			    ((DataRecordCol)i.next()).reset();
			}
		}
	}

	@Override
	public void postExecute() {
		for (Iterator<DataRecordCol> i = recordColList.iterator(); i.hasNext();) {
		    ((DataRecordCol)i.next()).free();
		}
	}

	@Override
	public void free() {
	    recordColList.clear();
	}

	/**
	 *  Rewinds the record pointer so the sorted records can be read again.
	 */
	public void rewind() {
		lastFound = 0;
		for (Iterator<DataRecordCol> i = recordColList.iterator(); i.hasNext();) {
		    ((DataRecordCol)i.next()).rewind();
		}
	}

	@Override
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

	private final boolean secureSpace(){
	    if (currentColIndex+1 >= numCollections){
	        return false;
	    }
	    /* add record collection (if it doesn't exist already)
	     * if it exists, just use it (we assume that it hase been reseted in such case) 
	    */  
	    currentColIndex++;
	    if (currentColIndex>=recordColList.size()){
	        // let's round it to 4B size (32bits)
	        currentColSize=(((currentColSize*COLLECTION_GROW_FACTOR)/10)/32+1)*32;
            try{
                currentRecordCol = new DataRecordCol(currentColSize);
            }catch(OutOfMemoryError ex){
                currentRecordCol = null;
                throw new RuntimeException("Out of memory in internal sorter algorithm. Please, set maximum Java heap size via -Xmx<size> JVM command line parameter or decrease sorter buffer capacity.");
            }
	        recordColList.add(currentRecordCol);
	    }else{
	        currentRecordCol=(DataRecordCol)recordColList.get(currentColIndex);
	    }
	    return true;
	}
	
	@Override
	public void sort() {
        if (useCollator){
            comparator=new RecordOrderedComparator(key.getKeyFields(), this.sortOrderings, collator);
            comparator.updateCollators(metadata);
        }else{
            comparator=new RecordOrderedComparator(key.getKeyFields(), this.sortOrderings);
        }
        comparator.setEqualNULLs(true);
        DataRecordCol recordArray;
	    for (Iterator<DataRecordCol> iterator = recordColList.iterator(); iterator.hasNext();) {
	        recordArray=((DataRecordCol)iterator.next());
	        // sort it now
	        java.util.Arrays.sort(recordArray.getRecordArray(), 0, recordArray.noItems,comparator);
	        recordArray.rewind(); // rewind to position reader pointer properly (first/last depending on sort order)
	    }
	    // for faster access, convert list to array
	    recordColArray=(DataRecordCol[])recordColList.toArray(new DataRecordCol[0]);
	}

	@Override
	public DataRecord get() {
	    // optimization - if only 1 sorted buffer, then no merge sorting
	    if (recordColArray.length==1){
	        return recordColArray[0].get();
	    }
	    /*if (sortOrderAscending){*/
	        return getNextLowest();
	    /*}else{
	        return getNextHighest();
	    }*/
	}

	@Override
	public boolean get(CloverBuffer recordDataBuffer) {
		DataRecord record=get();
		if (record!=null){
		    record.serialize(recordDataBuffer);
		    recordDataBuffer.flip();
		    return true;
		}else{
		    return false;
		}
	}

	@Override
	@Deprecated
	public boolean get(ByteBuffer recordDataBuffer) {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(recordDataBuffer);
		boolean result = get(wrappedBuffer);
		if (wrappedBuffer.buf() != recordDataBuffer) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
		return result;
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
		int pointer;
		int noItems;
		
		
		DataRecordCol(int capacity) {
			recordArray=new DataRecord[capacity];
			noItems=0;
			pointer=0;
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
	
		DataRecord get(){
	        if (pointer<noItems){
	            return recordArray[pointer++];
	        }else{
	            return null;
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
		    pointer=0;
		}
		
		void reset(){
			pointer=noItems=0;
		}
		
		void free(){
			reset();
			Arrays.fill(recordArray,null);
		}
		
	}

    /**
     * Determines whether Collator will be used
     * for sorting Strings.<br>
     * Without Collator, strings are sorted
     * based on individual characters unicode. With
     * Collator, locale-sensitive String comparison
     * is performed.
     * 
     * @param useCollator the useCollator to set
     */
	@Deprecated
    public void setUseCollator(boolean useCollator) {
        this.useCollator = useCollator;
    }


    /**
     * Set which Locale (national peculiarities) will be
     * used when comparing Strings.<br>
     * For example, in Czech  "ch" is a one character and
     * "a","??" both precede "b".
     * 
     * @param collatorLocale the collatorLocale to set
     */
	@Deprecated
    public void setCollatorLocale(Locale collatorLocale) {
		useCollator = true;
		collator = (RuleBasedCollator) Collator.getInstance(collatorLocale);
    }

    /**
     * Set which Locale (national peculiarities) will be
     * used when comparing Strings.<br>
     * For example, in Czech  "ch" is a one character and
     * "a","??" both precede "b".
     * 
     * @param collatorLocale    String representation of locale - e.g. "uk" or "fr"
     */
	@Deprecated
    public void setCollatorLocale(String collatorLocale) {
		setCollatorLocale(MiscUtils.createLocale(collatorLocale));
    }

    /**
	 * @param caseSensitive the caseSensitive to set
	 */
	@Deprecated
	public void setCaseSensitive(boolean caseSensitive) {
		if (collator == null) {
			collator =  (RuleBasedCollator) Collator.getInstance(MiscUtils.getDefaultLocale());
		}
		collator.setStrength(caseSensitive ? Collator.TERTIARY : Collator.SECONDARY);
	}
	
    /**
     * @param metaData
     * @param keys
     * @return
     */
    private void updateUseCollatorIndicator(DataRecordMetadata metaData, String[] keys) {
    	if (metaData.getLocaleStr() != null) {
    		useCollator = true;
    		return;
    	}
		for (int i = 0; i < keys.length; i++) {
			if (metaData.getField(keys[i]).getLocaleStr() != null) {
				useCollator = true;
				return;
			}
		}
    }
    
    public RuleBasedCollator getCollator() {
    	return collator;
    }
    
    public RecordOrderedComparator getComparator() {
    	return comparator;
    }
    
}
