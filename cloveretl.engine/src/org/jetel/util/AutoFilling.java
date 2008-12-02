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
    
    // names of functions for gui
    public static final String[] AUTOFILLING = new String[] {DEFAULT_VALUE, GLOBAL_ROW_COUNT, SOURCE_ROW_COUNT, METADATA_ROW_COUNT, 
    	METADATA_SOURCE_ROW_COUNT, SOURCE_NAME, SOURCE_TIMESTAMP, SOURCE_SIZE, ERROR_CODE, ERROR_MESSAGE};
    
    // autofilling for metadata
    private Map<DataRecordMetadata, AutoFillingData> autoFillingMap = new HashMap<DataRecordMetadata, AutoFillingData>();
    private AutoFillingData autoFillingData;

    // variables for global functions
    private Date fileTimestamp;
    private long fileSize;
	private int globalCounter; //number of returned records
	private int sourceCounter; //number of returned records in one source
    private String filename;

    // data object
	private static class AutoFillingData {
	    private int[] globalRowCount;	// number of returned records for every getNext method
	    private int[] sourceRowCount;
	    private int[] sourceName;
	    private int[] sourceTimestamp;
	    private int[] sourceSize;
	    private int[] defaultValue;

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
        autoFillingMap.put(metadata, createAutoFillingFields(metadata));
    }

    /**
     * Creates data object for autofilling.
     * @param metadata
     * @return
     */
    private AutoFillingData createAutoFillingFields(DataRecordMetadata metadata) {
        int numFields = metadata.getNumFields();
        int[] globalRowCountTmp = new int[numFields];
        int[] sourceRowCountTmp = new int[numFields];
        int[] metadataRowCountTmp = new int[numFields];
        int[] metadataSourceRowCountTmp = new int[numFields];
        int[] sourceNameTmp = new int[numFields];
        int[] sourceTimestampTmp = new int[numFields];
        int[] sourceSizeTmp = new int[numFields];
        int[] defaultValueTmp = new int[numFields];
        AutoFillingData data = new AutoFillingData();
        int globalRowCountLen = 0;
        int sourceNameLen = 0;
        int sourceTimestampLen = 0;
        int sourceSizeLen = 0;
        int defaultLen = 0;
	    int sourceRowCountLen = 0;
	    int metadataRowCountLen = 0;
	    int metadataSourceRowCountLen = 0;
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
        	}
        }
        data.globalRowCount = new int[globalRowCountLen];
        data.sourceRowCount = new int[sourceRowCountLen];
        data.metadataRowCount = new int[metadataRowCountLen];
        data.metadataSourceRowCount = new int[metadataSourceRowCountLen];
        data.sourceName = new int[sourceNameLen];
        data.sourceTimestamp = new int[sourceTimestampLen];
        data.sourceSize = new int[sourceSizeLen];
        data.defaultValue = new int[defaultLen];
        // reduce arrays' sizes
        System.arraycopy(globalRowCountTmp, 0, data.globalRowCount, 0, globalRowCountLen);
        System.arraycopy(sourceRowCountTmp, 0, data.sourceRowCount, 0, sourceRowCountLen);
        System.arraycopy(metadataRowCountTmp, 0, data.metadataRowCount, 0, metadataRowCountLen);
        System.arraycopy(metadataSourceRowCountTmp, 0, data.metadataSourceRowCount, 0, metadataSourceRowCountLen);
        System.arraycopy(sourceNameTmp, 0, data.sourceName, 0, sourceNameLen);
        System.arraycopy(sourceTimestampTmp, 0, data.sourceTimestamp, 0, sourceTimestampLen);
        System.arraycopy(sourceSizeTmp, 0, data.sourceSize, 0, sourceSizeLen);
        System.arraycopy(defaultValueTmp, 0, data.defaultValue, 0, defaultLen);
        
        return data;
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
        globalCounter++;
        sourceCounter++;
        autoFillingData.counter++;
        autoFillingData.sourceCounter++;
	}

	/**
	 * Reset method.
	 */
	public void reset() {
		globalCounter=0;
		sourceCounter=0;
		autoFillingMap.clear();
		autoFillingData = null;
	}

	public void setSourceCounter(int i) {
		for (Object autoFillingData : autoFillingMap.entrySet()) {
			((AutoFillingData)((Entry<?, ?>)autoFillingData).getValue()).sourceCounter = 0;
		}
	}

	public void setGlobalSourceCounter(int i) {
		sourceCounter=0;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public void setFileTimestamp(Date fileTimestamp) {
		this.fileTimestamp = fileTimestamp;
	}

	public void incGlobalCounter() {
		globalCounter++;
	}

	public final int getGlobalCounter() {
		return globalCounter;
	}

	public void incSourceCounter() {
		sourceCounter++;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

}
