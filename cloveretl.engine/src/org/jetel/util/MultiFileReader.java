/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
package org.jetel.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.WcardPattern;

/**
 * A class for transparent reading of clover data records from multiple input files.
 * The nested parser is used for parsing all input source files.
 * Usage: 
 * - first instantiate some suitable parser, set all its parameters (don't call init method)
 * - optionally set appropriate logger
 * - sets required multifile reader parameters (setFileSkip(), setSkip(), setNumRecords(), ...)
 * - call init method with metadata for reading input sources
 * - at last one can use this reader in the same way as all parsers via nextRecord method called in cycle
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 1.11.2006
 */
public class MultiFileReader {
	
    private static Log defaultLogger = LogFactory.getLog(MultiFileReader.class);
    private static final String STD_IN = "-";
    private Log logger = defaultLogger;

	private Parser parser;
    private URL contextURL;
    private String fileURL;
	private Iterator<String> filenameItor;
    private int skip;
	private int fileSkip;
	private int globalCounter; //number of returned records
	private int sourceCounter; //number of returned records in one source
	private int numRecords; //max number of returned records
    private boolean noInputFile = false;
    
    private String filename;
    private Date fileTimestamp;
    private long fileSize;
    private Map<DataRecordMetadata, AutoFillingData> autoFillingMap;
    private AutoFillingData autoFillingData;
    
    private static final String GLOBAL_ROW_COUNT = "global_row_count";
    private static final String SOURCE_ROW_COUNT = "source_row_count";
    private static final String METADATA_ROW_COUNT = "metadata_row_count";
    private static final String METADATA_SOURCE_ROW_COUNT = "metadata_source_row_count";
    private static final String SOURCE_NAME = "source_name";
    private static final String SOURCE_TIMESTAMP = "source_timestamp";
    private static final String SOURCE_SIZE = "source_size";
    private static final String DEFAULT_VALUE = "default_value";
    
    public static final String[] AUTOFILLING = new String[] {DEFAULT_VALUE, GLOBAL_ROW_COUNT, SOURCE_ROW_COUNT, METADATA_ROW_COUNT, 
    	METADATA_SOURCE_ROW_COUNT, SOURCE_NAME, SOURCE_TIMESTAMP, SOURCE_SIZE, "ErrCode", "ErrText"};
    
    /**
	 * Sole ctor.
	 * @param parser Parser to be used to obtain records from input files.
	 * @param fileURL Specification of input file(s)
	 */
	public MultiFileReader(Parser parser, URL contextURL, String fileURL) {
		this.parser = parser;
        this.contextURL = contextURL;
		this.fileURL = fileURL;
	}

    /**
     * Initialization of multi file reader. Calls parser.init() with a given metadata.
     * Tries open first data source.
     * @param metadata
     * @throws ComponentNotReadyException
     */
    public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
        parser.init(metadata);
        autoFillingMap = new HashMap<DataRecordMetadata, AutoFillingData>();
    	if (metadata != null) autoFillingData = addAutoFillingFields(metadata);
        
        WcardPattern pat = new WcardPattern();
        pat.addPattern(fileURL, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
        this.filenameItor = pat.filenames().iterator();

        try {
            if(!nextSource()) {
                noInputFile = true;
                throw new ComponentNotReadyException("FileURL attribute (" + fileURL + ") doesn't contain valid file url.");
            }
        } catch (JetelException e) {
            noInputFile = true;
            throw new ComponentNotReadyException("FileURL attribute (" + fileURL + ") doesn't contain valid file url.");
        }
    }

    /**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = skip;
    }
    
    /**
     * Sets number of skipped records in each file.
     * @param fileSkip
     */
    public void setFileSkip(int fileSkip) {
        this.fileSkip = fileSkip;
    }
    
    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }
    
    public void setLogger(Log logger) {
        this.logger = logger;
    }
    
	/**
     * Switch to the next source file.
	 * @return
	 * @throws JetelException 
	 */
	private boolean nextSource() throws JetelException {
		ReadableByteChannel stream = null; 
		while (filenameItor.hasNext()) {
			filename = filenameItor.next();
			for (Object autoFillingData : autoFillingMap.entrySet()) {
				((AutoFillingData)((Entry)autoFillingData).getValue()).sourceCounter = 0;
			}
			sourceCounter = 0;
			logger.debug("Opening input file " + filename);
			try {
				stream = FileUtils.getReadableChannel(contextURL, filename);
				File tmpFile = new File(filename);
				long timestamp = tmpFile.lastModified();
				fileSize = tmpFile.length();
				fileTimestamp = timestamp == 0 ? null : new Date(timestamp);				
				logger.debug("Reading input file " + filename);
				parser.setReleaseDataSource(!filename.equals(STD_IN));
				parser.setDataSource(stream);
				if(fileSkip > 0) parser.skip(fileSkip);
				return true;
			} catch (IOException e) {
				throw new JetelException("File is unreachable: " + filename, e);
			} catch (JetelException e) {
				logger.error("An error occured while skipping records in file " + filename + ", the file will be ignored", e);
				continue;
			} catch (ComponentNotReadyException e) {
			    logger.error("An error occured while switching input file " + filename + ", the file will be ignored", e);
                continue;
            }
		}
		return false;
	}

	/**
	 * This private method try to skip records given in <code>skip</code> variable.
     * @param skip number of skipped records
	 * @throws JetelException 
	 */
	private void skip(int skip) throws JetelException {
        int skipped = 0;

        do {
            try {
                skipped += parser.skip(skip - skipped);            
            } catch (JetelException e) {
                logger.error("An error occured while skipping records in file " + filename + ", the file will be ignored", e);
            }
        } while (skipped < skip && nextSource());
    }
    
	/**
	 * Tries to obtain one record
	 * @param record Instance to be filled with obtained data
	 * @return null on error, the record otherwise
	 * @throws JetelException
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
        //in case that fileURL doesn't contain valid file url
        if(noInputFile) {
            return null;
        }
        
        //check for index of last returned record
        if(numRecords > 0 && numRecords == globalCounter) {
            return null;
        }
        
        //shall i skip some records?
        if(skip > 0) {
            skip(skip);
            skip = 0;
        }
        
        //use parser to get next record
        DataRecord rec;
        try {
            while((rec = parser.getNext(record)) == null && nextSource());
        } catch(JetelException e) {
            globalCounter++;
            sourceCounter++;
            throw e;
        }
        if(rec != null) {
            autoFillingData = autoFillingMap.get(rec.getMetadata());
            if (autoFillingData == null) {
            	autoFillingData = addAutoFillingFields(rec.getMetadata());
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
        
        return rec;
	}		

	/**
	 * Tries to obtain one record
	 * @param record Instance to be filled with obtained data
	 * @return null on error, the record otherwise
	 * @throws JetelException
	 */
	public DataRecord getNext() throws JetelException {
        //in case that fileURL doesn't contain valid file url
        if(noInputFile) {
            return null;
        }
        
        //check for index of last returned record
        if(numRecords > 0 && numRecords == globalCounter) {
            return null;
        }
        
        //shall i skip some records?
        if(skip > 0) {
            skip(skip);
            skip = 0;
        }
        
        //use parser to get next record
        DataRecord rec;
        try {
            while((rec = parser.getNext()) == null && nextSource());
        } catch(JetelException e) {
            globalCounter++;
            throw e;
        }
        if(rec != null) {
            autoFillingData = autoFillingMap.get(rec.getMetadata());
            if (autoFillingData == null) {
            	autoFillingData = addAutoFillingFields(rec.getMetadata());
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
        return rec;
	}		

	/**
	 * Releases resources held by the instance
	 *
	 */
	public void close() {
		parser.close();
	}
	
	private class AutoFillingData {
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
	
    private AutoFillingData addAutoFillingFields(DataRecordMetadata metadata) {
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
        
        autoFillingMap.put(metadata, data);
        return data;
    }

}
