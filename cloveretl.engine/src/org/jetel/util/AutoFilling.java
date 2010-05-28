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
package org.jetel.util;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Provides the support for 'Autofilling' functions.
 */
public class AutoFilling {

	// names of functions
    private static final String GLOBAL_READER_ROW_COUNT = "graph_reader_row_count";
    private static final String GLOBAL_ROW_COUNT = "global_row_count";
    private static final String SOURCE_ROW_COUNT = "source_row_count";
    private static final String METADATA_ROW_COUNT = "metadata_row_count";
    private static final String METADATA_SOURCE_ROW_COUNT = "metadata_source_row_count";
    private static final String SOURCE_NAME = "source_name";
    private static final String SOURCE_TIMESTAMP = "source_timestamp";
    private static final String SOURCE_SIZE = "source_size";
    private static final String DEFAULT_VALUE = "default_value";
    public static final String ERROR_CODE = "ErrCode";
    public static final String ERROR_MESSAGE = "ErrText";
    private static final String ROW_TIMESTAMP = "row_timestamp";
    private static final String READER_TIMESTAMP = "reader_timestamp";
    public static final String SHEET_NAME = "sheet_name";					// is used in XLSParser
    
    // names of functions for gui
    public static final String[] AUTOFILLING = new String[] {DEFAULT_VALUE, GLOBAL_READER_ROW_COUNT, GLOBAL_ROW_COUNT, SOURCE_ROW_COUNT, METADATA_ROW_COUNT, 
    	METADATA_SOURCE_ROW_COUNT, SOURCE_NAME, SOURCE_TIMESTAMP, SOURCE_SIZE, ROW_TIMESTAMP, READER_TIMESTAMP, ERROR_CODE, ERROR_MESSAGE, SHEET_NAME};
    
    // autofilling for metadata
    private Map<DataRecordMetadata, AutoFillingData> autoFillingMap = new HashMap<DataRecordMetadata, AutoFillingData>();
    private AutoFillingData autoFillingData;

    // variables for global functions
    private Date fileTimestamp;
    private long fileSize;
	private static volatile int globalReaderCounter; //number of returned records for all readers
	private int globalCounter; //number of returned records
	private int sourceCounter; //number of returned records in one source
    private String filename;
    private Date readerTimestamp;

    // data object
	private static class AutoFillingData {
		
		private boolean noAutoFillingData; // the attribute indicates if metadata doesn't contain any autofilling field
		
	    private int[] globalReaderRowCount;
	    private int[] globalRowCount;	// number of returned records for every getNext method
	    private int[] sourceRowCount;
	    private int[] sourceName;
	    private int[] sourceTimestamp;
	    private int[] sourceSize;
	    private int[] defaultValue;
	    private int[] rowTimestamp;
	    private int[] readerTimestamp;

	    private int counter; // number of returned records for one metadata
	    private int[] metadataRowCount;
	    
		private int sourceCounter; // number of returned records in one source for one metadata
	    private int[] metadataSourceRowCount;
	}
	
	/**
	 * Creates data object for autofilling.
	 * @param metadata
	 */
    public void addAutoFillingFields(DataRecordMetadata metadata) {
    	// create and put new autofilling
        autoFillingMap.put(metadata, autoFillingData = createAutoFillingFields(metadata));
    }

    /**
     * Creates data object for autofilling.
     * @param metadata
     * @return
     */
    private AutoFillingData createAutoFillingFields(DataRecordMetadata metadata) {
        int numFields = metadata.getNumFields();
        int[] globalReaderRowCountTmp = new int[numFields];
        int[] globalRowCountTmp = new int[numFields];
        int[] sourceRowCountTmp = new int[numFields];
        int[] metadataRowCountTmp = new int[numFields];
        int[] metadataSourceRowCountTmp = new int[numFields];
        int[] sourceNameTmp = new int[numFields];
        int[] sourceTimestampTmp = new int[numFields];
        int[] sourceSizeTmp = new int[numFields];
        int[] defaultValueTmp = new int[numFields];
        int[] rowTimestampTmp = new int[numFields];
        int[] readerTimestampTmp = new int[numFields];
        AutoFillingData data = new AutoFillingData();
        int globalReaderRowCountLen = 0;
        int globalRowCountLen = 0;
        int sourceNameLen = 0;
        int sourceTimestampLen = 0;
        int sourceSizeLen = 0;
        int defaultLen = 0;
	    int sourceRowCountLen = 0;
	    int metadataRowCountLen = 0;
	    int metadataSourceRowCountLen = 0;
        int rowTimestampLen = 0;
        int readerTimestampLen = 0;
        for (int i=0; i<numFields; i++) {
        	if (metadata.getField(i).getAutoFilling() != null) {
        		if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(GLOBAL_ROW_COUNT)) globalRowCountTmp[globalRowCountLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(SOURCE_ROW_COUNT)) sourceRowCountTmp[sourceRowCountLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(METADATA_ROW_COUNT)) metadataRowCountTmp[metadataRowCountLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(METADATA_SOURCE_ROW_COUNT)) metadataSourceRowCountTmp[metadataSourceRowCountLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(SOURCE_NAME)) sourceNameTmp[sourceNameLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(SOURCE_TIMESTAMP)) sourceTimestampTmp[sourceTimestampLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(SOURCE_SIZE)) sourceSizeTmp[sourceSizeLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(DEFAULT_VALUE)) defaultValueTmp[defaultLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(ROW_TIMESTAMP)) rowTimestampTmp[rowTimestampLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(READER_TIMESTAMP)) readerTimestampTmp[readerTimestampLen++] = i;
        		else if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(GLOBAL_READER_ROW_COUNT)) globalReaderRowCountTmp[globalReaderRowCountLen++] = i;
        	}
        }
        data.globalReaderRowCount = new int[globalReaderRowCountLen];
        data.globalRowCount = new int[globalRowCountLen];
        data.sourceRowCount = new int[sourceRowCountLen];
        data.metadataRowCount = new int[metadataRowCountLen];
        data.metadataSourceRowCount = new int[metadataSourceRowCountLen];
        data.sourceName = new int[sourceNameLen];
        data.sourceTimestamp = new int[sourceTimestampLen];
        data.sourceSize = new int[sourceSizeLen];
        data.defaultValue = new int[defaultLen];
        data.rowTimestamp = new int[rowTimestampLen];
        data.readerTimestamp = new int[readerTimestampLen];
        data.noAutoFillingData = 
        	globalReaderRowCountLen <= 0 &&
        	globalRowCountLen <= 0 &&
        	sourceRowCountLen <= 0 &&
        	metadataRowCountLen <= 0 &&
        	metadataSourceRowCountLen <= 0 &&
        	sourceNameLen <= 0 &&
        	sourceTimestampLen <= 0 &&
        	sourceSizeLen <= 0 &&
        	rowTimestampLen <= 0 &&
        	readerTimestampLen <= 0 &&
        	defaultLen <= 0;

        // reduce arrays' sizes
        System.arraycopy(globalReaderRowCountTmp, 0, data.globalReaderRowCount, 0, globalReaderRowCountLen);
        System.arraycopy(globalRowCountTmp, 0, data.globalRowCount, 0, globalRowCountLen);
        System.arraycopy(sourceRowCountTmp, 0, data.sourceRowCount, 0, sourceRowCountLen);
        System.arraycopy(metadataRowCountTmp, 0, data.metadataRowCount, 0, metadataRowCountLen);
        System.arraycopy(metadataSourceRowCountTmp, 0, data.metadataSourceRowCount, 0, metadataSourceRowCountLen);
        System.arraycopy(sourceNameTmp, 0, data.sourceName, 0, sourceNameLen);
        System.arraycopy(sourceTimestampTmp, 0, data.sourceTimestamp, 0, sourceTimestampLen);
        System.arraycopy(sourceSizeTmp, 0, data.sourceSize, 0, sourceSizeLen);
        System.arraycopy(defaultValueTmp, 0, data.defaultValue, 0, defaultLen);
        System.arraycopy(rowTimestampTmp, 0, data.rowTimestamp, 0, rowTimestampLen);
        System.arraycopy(readerTimestampTmp, 0, data.readerTimestamp, 0, readerTimestampLen);

        return data;
    }

	/**
	 * Sets autofilling fields in data record for the last used metadata. You can set by addAutoFillingFields or setAutoFillingFields method.
	 * 
	 * @param rec
	 */
	public void setLastUsedAutoFillingFields(DataRecord rec) {
        if(rec == null) return;
        setAutofilling(rec);
	}

	/**
	 * Sets autofilling fields in data record.
	 * 
	 * @param rec
	 */
	public void setAutoFillingFields(DataRecord rec) {
        if(rec == null) return;
        autoFillingData = autoFillingMap.get(rec.getMetadata());
        if (autoFillingData == null) {
        	autoFillingData = createAutoFillingFields(rec.getMetadata());
            autoFillingMap.put(rec.getMetadata(), autoFillingData);
        }
        setAutofilling(rec);
	}

	/**
	 * Fill in the autofilling data.
	 * @param rec
	 */
	private final void setAutofilling(DataRecord rec) {
        if (autoFillingData.noAutoFillingData) {
            sourceCounter++;
            globalCounter++;
        	return;
        }
        
        if (autoFillingData.globalReaderRowCount.length > 0) {
        	setValuesAndIncCounter(autoFillingData, rec);
        }
       	for (int i : autoFillingData.globalRowCount) {
       		rec.getField(i).setValue(globalCounter);
       	}
       	for (int i : autoFillingData.sourceRowCount) {
       		rec.getField(i).setValue(sourceCounter);
       	}
       	for (int i : autoFillingData.metadataRowCount) {
       		rec.getField(i).setValue(autoFillingData.counter);
       	}
       	for (int i : autoFillingData.metadataSourceRowCount) {
       		rec.getField(i).setValue(autoFillingData.sourceCounter);
       	}
       	for (int i : autoFillingData.sourceName) {
       		rec.getField(i).setValue(filename);
       	}
       	for (int i : autoFillingData.sourceTimestamp) {
       		rec.getField(i).setValue(fileTimestamp);
       	}
       	for (int i : autoFillingData.sourceSize) {
       		rec.getField(i).setValue(fileSize);
       	}
       	for (int i : autoFillingData.defaultValue) {
       		rec.getField(i).setToDefaultValue();
       	}
       	for (int i : autoFillingData.readerTimestamp) {
       		rec.getField(i).setValue(readerTimestamp == null ? readerTimestamp = new Date() : readerTimestamp);
       	}
       	for (int i : autoFillingData.rowTimestamp) {
       		rec.getField(i).setValue(new Date());
       	}
        globalCounter++;
        sourceCounter++;
        autoFillingData.counter++;
        autoFillingData.sourceCounter++;
	}

	/**
	 * This synchronized method sets and increases graph_reader_row_count. 
	 */
	private static synchronized void setValuesAndIncCounter(AutoFillingData autoFillingData, DataRecord rec) {
       	for (int i : autoFillingData.globalReaderRowCount) {
       		rec.getField(i).setValue(globalReaderCounter);
       	}
       	globalReaderCounter++;
	}
	
	
	/**
	 * Reset method.
	 */
	public void reset() {
       	globalReaderCounter=0;
		globalCounter=0;
		sourceCounter=0;
		autoFillingMap.clear();
	}

	/**
	 * Sets source counter to 0.
	 */
	public void resetSourceCounter() {
		for (Object autoFillingData : autoFillingMap.entrySet()) {
			((AutoFillingData)((Entry<?, ?>)autoFillingData).getValue()).sourceCounter = 0;
		}
	}

	/**
	 * Sets global source counter to 0.
	 */
	public void resetGlobalSourceCounter() {
		sourceCounter=0;
	}

	/**
	 * Sets file or source size for a current source.
	 * @param fileSize
	 */
	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	/**
	 * Sets file or source time stamp for a current source.
	 * @param fileTimestamp
	 */
	public void setFileTimestamp(Date fileTimestamp) {
		this.fileTimestamp = fileTimestamp;
	}

	/**
	 * Adds 1 to the global counter.
	 */
	public void incGlobalCounter() {
		globalCounter++;
	}

	/**
	 * Gets global counter.
	 * @return
	 */
	public final int getGlobalCounter() {
		return globalCounter;
	}

	/**
	 * Gets source counter.
	 * @return
	 */
	public final int getSourceCounter() {
		return sourceCounter;
	}

	/**
	 * Adds 1 to the source counter.
	 */
	public void incSourceCounter() {
		sourceCounter++;
	}

	/**
	 * Gets a file or source name.
	 * @return
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * Sets a file or source name.
	 * @param filename
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

}
