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
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;

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
	private ReadableChannelIterator channelIterator;
    private int skip;
    private int fSkip;
	private int fileSkip;
	private int globalCounter; //number of returned records
	private int sourceCounter; //number of returned records in one source
	private int numRecords; //max number of returned records
    private boolean noInputFile = false;
    private String incrementalFile;
    private String incrementalKey;
    private static Map<String, Incremental> incrementalProperties;
    private String[] incrementalInValues;
    private ArrayList<String> incrementalOutValues;
    private int iSource;

    private String filename;
    private Date fileTimestamp;
    private long fileSize;
    private Map<DataRecordMetadata, AutoFillingData> autoFillingMap;
    private AutoFillingData autoFillingData;
    
    private InputPort inputPort;
	private String charset;
	private Dictionary dictionary;
	private boolean initializeDataDependentSource;
    
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
		initIncrementalReading();
        parser.init(metadata);
        autoFillingMap = new HashMap<DataRecordMetadata, AutoFillingData>();
    	if (metadata != null) autoFillingData = addAutoFillingFields(metadata);
		iSource = -1;
        
		initChannelIterator();
        try {
            if(!(initializeDataDependentSource = channelIterator.isGraphDependentSource()) && !nextSource()) {
                noInputFile = true;
                //throw new ComponentNotReadyException("FileURL attribute (" + fileURL + ") doesn't contain valid file url.");
            }
        } catch (JetelException e) {
            noInputFile = true;
            throw new ComponentNotReadyException("FileURL attribute (" + fileURL + ") doesn't contain valid file url.");
        }
    }

    private void initChannelIterator() throws ComponentNotReadyException {
    	channelIterator = new ReadableChannelIterator(inputPort, contextURL, fileURL);
    	channelIterator.setCharset(charset);
    	channelIterator.setDictionary(dictionary);
    	channelIterator.init();
    }
    
    /**
     * Sets an input port for data reading from input record.
     * 
     * @param metadata
     */
    public void setInputPort(InputPort inputPort) {
    	this.inputPort = inputPort;
    }

    /**
     * @param metadata
     * @throws ComponentNotReadyException
     */
	public void checkConfig(DataRecordMetadata metadata) throws ComponentNotReadyException {
        parser.init(metadata);
        initChannelIterator();
        
		String fName; 
		Iterator<String> fit = channelIterator.getFileIterator();
		while (fit.hasNext()) {
			try {
				fName = fit.next();
				URL url = FileUtils.getInnerAddress(fName);
				if (FileUtils.isServerURL(url)) {
					//FileUtils.checkServer(url); //this is very long operation
					continue;
				}
				parser.setReleaseDataSource(!fName.equals(STD_IN));
				parser.setDataSource(FileUtils.getReadableChannel(contextURL, fName));
			} catch (IOException e) {
				throw new ComponentNotReadyException("File is unreachable: " + filename, e);
			} catch (ComponentNotReadyException e) {
				throw new ComponentNotReadyException("File is unreachable: " + filename, e);
			}
		}
	}
	
    /**
     * Initializes incremental reading.
     * @throws ComponentNotReadyException
     */
    private void initIncrementalReading() throws ComponentNotReadyException {
    	if (incrementalFile == null && incrementalKey != null) throw new ComponentNotReadyException("Incremental file is not defined for the '" + incrementalKey + "' incremental key attribute!");
    	if (incrementalFile != null && incrementalKey == null) throw new ComponentNotReadyException("Incremental key is not defined for the '" + incrementalFile + "' incremental file attribute!");
    	
    	if (incrementalFile == null) return;
    	if (incrementalProperties == null) {
    		incrementalProperties = new HashMap<String, Incremental>();
    	}
    	Incremental incremental;
    	Properties prop = new Properties();
    	try {
    		prop.load(Channels.newInputStream(FileUtils.getReadableChannel(contextURL, incrementalFile)));
		} catch (IOException e) {
			logger.warn("The incremental file not found or it is corrupted! Cause: " + e.getMessage());
		}
		incremental = new Incremental(prop);
		incremental.add(incrementalKey);
		incrementalProperties.put(incrementalFile, incremental);
    	
		String incrementalValue = (String) prop.get(incrementalKey);
		if (incrementalValue == null) {
			logger.warn("The incremental key '" + incrementalKey + "' not found!");
		}
		incrementalInValues = incrementalValue != null && !incrementalValue.equals("") ? incrementalValue.split(";") : new String[0];
		incrementalOutValues = new ArrayList<String>();
		for (String value: incrementalInValues) {
			incrementalOutValues.add(value);
		}
		try {
			storeIncrementalReading();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
    }

    /**
     * Resets incremental reading. 
     * @throws IOException 
     */
    private void resetIncrementalReading() throws IOException {
    	storeIncrementalReading();
		if (incrementalOutValues != null) {
			incrementalInValues = new String[incrementalOutValues.size()];
			incrementalOutValues.toArray(incrementalInValues);
		}
    }
    
	/**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = fSkip = skip;
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
		// update incremental value from previous source
		if (incrementalOutValues != null && iSource >= 0) {
			for (int i=incrementalOutValues.size(); i<iSource; i++) incrementalOutValues.add(null);
			Object position = parser.getPosition();
			if (iSource < incrementalOutValues.size()) incrementalOutValues.remove(iSource);
			incrementalOutValues.add(iSource, position != null ? position.toString() : null);
		}
		// next source
		ReadableByteChannel stream = null;
		while (channelIterator.hasNext()) {
			for (Object autoFillingData : autoFillingMap.entrySet()) {
				((AutoFillingData)((Entry<?, ?>)autoFillingData).getValue()).sourceCounter = 0;
			}
			sourceCounter = 0;
			try {
				stream = channelIterator.next();
				if (stream == null) continue; // if record no record found
				filename = channelIterator.getCurrentFileName();
				File tmpFile = new File(filename);
				long timestamp = tmpFile.lastModified();
				fileSize = tmpFile.length();
				fileTimestamp = timestamp == 0 ? null : new Date(timestamp);				
				iSource++;
				parser.setReleaseDataSource(!filename.equals(STD_IN));
				parser.setDataSource(stream);
				if (incrementalInValues != null && iSource < incrementalInValues.length) {
					parser.movePosition(incrementalInValues[iSource]);
				}
				if(fileSkip > 0) parser.skip(fileSkip);
				return true;
			} catch (Exception e) {
				logger.error("An error occured while skipping records in file " + filename + ", the file will be ignored", e);
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
            initializeDataDependentSource();
            while((rec = parser.getNext(record)) == null && nextSource());
        } catch(JetelException e) {
            globalCounter++;
            sourceCounter++;
            throw e;
        }
        setAutoFillingFields(rec);
        
        if (rec == null) channelIterator.blankRead();
        
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
            initializeDataDependentSource();
            while((rec = parser.getNext()) == null && nextSource());
        } catch(JetelException e) {
            globalCounter++;
            throw e;
        }
        setAutoFillingFields(rec);
        
        if (rec == null) channelIterator.blankRead();
        
        return rec;
	}		

	private final void initializeDataDependentSource() throws JetelException {
        if (initializeDataDependentSource) {
        	nextSource();
        	initializeDataDependentSource = false;
        }
	}

	/**
	 * Sets autofilling fields in data record.
	 * 
	 * @param rec
	 */
	private void setAutoFillingFields(DataRecord rec) {
        if(rec == null) return;

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
	
	/**
	 * Updates and stores incremental reading values into a file.
	 * @throws IOException 
	 */
	public void storeIncrementalReading() throws IOException {
		if (incrementalFile == null || incrementalProperties == null) return;
		
		OutputStream os = Channels.newOutputStream(FileUtils.getWritableChannel(contextURL, incrementalFile, false));
		Properties prop = incrementalProperties.get(incrementalFile).getProperties();
		prop.remove(incrementalKey);
		StringBuilder sb = new StringBuilder();
		for (String value: incrementalOutValues) sb.append(value).append(";");
		if (sb.length() > 0) sb.deleteCharAt(sb.length()-1);
		prop.put(incrementalKey, sb.toString());
		prop.store(os, "Incremental reading properties");
		os.flush();
		os.close();
	}
	
	/**
	 * Releases resources held by the instance
	 *
	 */
	public void close() {
		parser.close();
	}
	
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

    /**
	 * Reset reader for next graph execution. 
     * @throws ComponentNotReadyException 
     */
	public void reset() throws ComponentNotReadyException {
		parser.reset();
		globalCounter=0;
		sourceCounter=0;
		noInputFile = false;
		autoFillingMap.clear();
		autoFillingData = null;
		iSource = -1;
		skip = fSkip;

		channelIterator.reset();
        try {
    		resetIncrementalReading();
			if(!(initializeDataDependentSource = channelIterator.isGraphDependentSource()) && !nextSource()) 
			    noInputFile = true;
		} catch (JetelException e) {
			logger.error("reset", e);
		} catch (IOException e) {
			logger.error("reset", e);
		}
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

    public void setIncrementalFile(String incrementalFile) {
    	this.incrementalFile = incrementalFile;
    }

    public void setIncrementalKey(String incrementalKey) {
    	this.incrementalKey = incrementalKey;
    }


    /**
     * The class for incremental reading.
     */
    private static class Incremental {
    	// properties for a file
    	private Properties properties;
    	// used keys
    	private Set<String> used;

    	/**
    	 * Constructor
    	 */
    	public Incremental(Properties properties) {
    		this.properties = properties;
    		used = new HashSet<String>();
    	}
    	
    	/**
    	 * properties for particular file
    	 */
    	public Properties getProperties() {
    		return properties;
    	}

    	public boolean contains(String key) {
    		return used.contains(key);
    	}
    	public void add(String key) {
    		used.add(key);
    	}
    }
}
