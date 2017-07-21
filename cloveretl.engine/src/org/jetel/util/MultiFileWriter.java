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

import java.io.IOException;
import java.net.URL;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.RecordKey;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.provider.FormatterProvider;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.enums.ArchiveType;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.OutputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Class for transparent writing into multifile or multistream. Underlying formatter is used for formatting
 * incoming data records and destination is a list of files defined in fileURL attribute
 * by org.jetel.util.MultiOutFile or iterator of writable channels.
 * The MultiFileWriter can partition data according to lookup table and partition key or partition key only.
 * Usage: 
 * - first instantiate some suitable formatter, set all its parameters (don't call init method)
 * - optionally set appropriate logger
 * - sets required multifile writer parameters (setAppendData(), setRecordsPerFile(), setBytesPerLine(), ...)
 * - call init method with metadata for reading input sources
 * - at last one can use this writer in the same way as all formatter via write method called in cycle

 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 2.11.2006
 */
@SuppressFBWarnings({"EI2","EI"})
public class MultiFileWriter {
	// Default capacity of HashMap
	private final static int tableInitialSize = 512;

    private Formatter currentFormatter;				// for former constructor
    private FormatterProvider formatterGetter;		// creates new formatter
    private Map<Object, TargetFile> multiTarget;	// <key, TargetFile> for file partition
    private TargetFile currentTarget;				// actual output target
	private TargetFile unassignedTarget;			// for lookup table if an input record doesn't have lookup record
    private URL contextURL;
    private String fileURL;
    private int recordsPerFile;
    private int bytesPerFile;
    private boolean appendData;
    private Iterator<WritableByteChannel> channels;
    /** counter which decreases with each skipped record */
    private int skip;
    /** fixed value */
    private int skipRecords;
	private int numRecords;
	private int counter;
	private boolean useChannel = true;
	private DataRecordMetadata metadata;
    
	private LookupTable lookupTable = null;
	private Lookup lookup;
	private String[] partitionKeyNames;
	private RecordKey partitionKey;
	private String[] partitionOutFields;
	private int[] iPartitionOutFields;
	private String unassignedFileURL;
	
	private boolean useNumberFileTag = true;
	private int numberFileTag;
	
	private OutputPort outputPort;
	private String charset;
	private Dictionary dictionary;
	private int compressLevel = -1;
	private boolean mkDir;
	private boolean outputClosed;
	
	private boolean reset;

	private boolean storeRawData = true;
	
	// CLO-1634
	private boolean createEmptyFiles = true;
	
	/**
	 * This switch is used to say to partitioning algorithm, that
	 * the incoming data records are sorted according partition key.
	 * So output data files does not need to be kept open all the time.
	 * Output files are generated after the each other.
	 */
	private boolean sortedInput = false;
	private String lastKeyForSortedInput;
	
    private boolean deserialized;
    private DataRecord record = null;
    
	
    /**
     * Constructor.
     * @param formatter formatter is used for incoming records formatting
     * @param fileURL target file(s) definition
     */
    public MultiFileWriter(Formatter formatter, URL contextURL, String fileURL) {
        this.currentFormatter = formatter;
        this.contextURL = contextURL;
        this.fileURL = fileURL;
    }

    public MultiFileWriter(Formatter formatter, Iterator<WritableByteChannel> channels) {
        this.currentFormatter = formatter;
        this.channels = channels;
    }

    public MultiFileWriter(FormatterProvider formatterGetter, URL contextURL, String fileURL) {
        this.formatterGetter = formatterGetter;
        this.contextURL = contextURL;
        this.fileURL = fileURL;
    }

    public MultiFileWriter(FormatterProvider formatterGetter, Iterator<WritableByteChannel> channels) {
        this.formatterGetter = formatterGetter;
        this.channels = channels;
    }

    /**
     * Initializes underlying formatter with a given metadata.
     * 
     * @param metadata
     * @throws ComponentNotReadyException 
     * @throws IOException 
     */
    public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
    	this.metadata = metadata;
    	preparePatitionKey();	// initialize partition key - if defined
    	prepareTargets();		// prepare output targets
    }
    
    /**
	 * Reset writer for next graph execution. 
     * @throws ComponentNotReadyException 
     */
	public void reset() throws ComponentNotReadyException {
		if (multiTarget != null){
			multiTarget.clear();
			currentTarget = null;
		} else if (currentTarget != null) { // CLO-7184
			currentTarget.reset();
		}
		counter = 0;
		numberFileTag = 0;
		skip = skipRecords;
		outputClosed = false;
    	preparePatitionKey();	// initialize partition key - if defined
    	reset = true;			//TODO delete in the 2.9, there is pre_execute
	}
	
    /**
     * Creates target array or target map.
     * 
     * @throws ComponentNotReadyException
     */
    private void prepareTargets() throws ComponentNotReadyException {
    	// prepare type of targets: lookup/keyValue
		try {
			if (partitionKey != null) {
				// CL-2564
				StringBuilder innerSource = new StringBuilder();
				StringBuilder anchor = new StringBuilder();
				URL url = FileUtils.getFileURL(contextURL, fileURL);
				ArchiveType archiveType = FileUtils.getArchiveType(url.toString(), innerSource, anchor);
				if ((archiveType != null) && !StringUtils.isEmpty(anchor) && (anchor.indexOf("#") >= 0)) {
					if (archiveType != ArchiveType.ZIP) {
						throw new ComponentNotReadyException("Partitioning within " + archiveType + " archives is not supported");
					} else {
						if (!FileUtils.isLocalArchiveOutputPath(contextURL, fileURL)) {
							throw new ComponentNotReadyException("Partitioning within remote ZIP archives is not supported: " + url);
						}
					}
				}
				
				multiTarget = new HashMap<Object, TargetFile>(tableInitialSize);
				
			// prepare type of targets: single
			} else if (createEmptyFiles) {
				prepareSingleTarget();
			}
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
    }

	/**
	 * @throws ComponentNotReadyException
	 * @throws IOException
	 */
	private void prepareSingleTarget() throws ComponentNotReadyException, IOException {
		if (currentFormatter == null) {
			currentFormatter = formatterGetter.getNewFormatter();	
		}
		currentFormatter.init(metadata);
		if (currentTarget != null)
			currentTarget.close();
		currentTarget = createNewTarget(currentFormatter);
		currentTarget.init();
	}
    
    /**
     * Creates new target according to fileURL or channels. 
     * Sets append data and uses channel properties.
     * 
     * @return target
     */
    private TargetFile createNewTarget() {
    	TargetFile targetFile;
		if (fileURL != null) targetFile = new TargetFile(fileURL, contextURL, formatterGetter, metadata);
		else targetFile = new TargetFile(channels, formatterGetter, metadata);
		targetFile.setAppendData(appendData);
		targetFile.setUseChannel(useChannel);
		targetFile.setCharset(charset);
		targetFile.setStoreRawData(storeRawData);
		targetFile.setCompressLevel(compressLevel);
		targetFile.setOutputPort(outputPort);
		targetFile.setDictionary(dictionary);
		targetFile.setMkDir(mkDir);
		return targetFile;
    }
    
    /**
     * Creates new target according to fileURL or channels for the formatter. 
     * Sets append data and uses channel properties.
     * 
     * @return target
     */
    private TargetFile createNewTarget(Formatter formatter) {
    	TargetFile targetFile;
		if (fileURL != null) 
			targetFile = new TargetFile(fileURL, contextURL, formatter, metadata);
		else 
			targetFile = new TargetFile(channels, formatter, metadata);
		targetFile.setAppendData(appendData);
		targetFile.setUseChannel(useChannel);
		targetFile.setCharset(charset);
		targetFile.setStoreRawData(storeRawData);
		targetFile.setCompressLevel(compressLevel);
		targetFile.setOutputPort(outputPort);
		targetFile.setDictionary(dictionary);
		targetFile.setMkDir(mkDir);
		return targetFile;
    }

    
    /**
     * Checks and prepares partition key. 
     * 
     * @param metadata
     * @throws ComponentNotReadyException
     */
    private void preparePatitionKey() throws ComponentNotReadyException {
    	if (partitionKeyNames == null && partitionKey == null && lookupTable != null) {
    		throw new ComponentNotReadyException("Lookup table is not properly defined. The partition key is missing.");
    	}
	    if (partitionKeyNames != null) {
			try {
				partitionKey = new RecordKey(partitionKeyNames, metadata);
				if (lookupTable != null) {
					lookup = lookupTable.createLookup(partitionKey);
				}
			} catch (Exception e) {
				throw new ComponentNotReadyException(e);
			}
	    }
    }
    
    private boolean writeCommon() throws IOException, ComponentNotReadyException {
    	if (reset) {
    		prepareTargets();
    		reset = false;
    	}

        // check for index of last returned record
        if(numRecords > 0 && numRecords == counter) {
            return false;
        }

        // shall i skip some records?
        if(skip > 0) {
            skip--;
            return false;
        }

        if (currentTarget != null) {
        	checkAndSetNextOutput();
        }
        
        return true;
    }
    
    /**
     * Returns the record passed as {@link CloverBuffer}
     * deserialized to a {@link DataRecord}.
     * 
     * Caches the result to avoid multiple deserialization
     * of the same record.
     * 
     * @param buffer - current raw record
     * 
     * @return buffer deserialized to a {@link DataRecord}
     */
    private DataRecord getCurrentRecord(CloverBuffer buffer) {
    	if (!deserialized) {
    		if (this.record == null) {
        		this.record = DataRecordFactory.newRecord(this.metadata);
    		}
    		record.deserialize(buffer);
    		buffer.rewind();
    		deserialized = true;
    	}
    	
    	return record;
    }
    
    public void writeDirect(CloverBuffer buffer) throws IOException, ComponentNotReadyException {
    	if (!writeCommon()) {
    		return;
    	}

    	this.deserialized = false; // new record - clear cache
    	
        // write the record according to value partition
        if (partitionKey == null) {
        	if (currentTarget == null) { // CLO-1634
        		prepareSingleTarget();
        	}
            // single formatter/getter
        	writeRecord2CurrentTarget(buffer);
        } else {
        	DataRecord outputRecord = getCurrentRecord(buffer);
        	if (lookupTable != null) {
                // write the record according to lookup table
            	writeRecord4LookupTable(outputRecord, buffer);
            } else {
            	// just partition key without lookup table
            	writeRecord2MultiTarget(outputRecord, buffer);
            }
        }
    }

    /**
     * Writes given record via formatter into destination file(s).
     * @param record
     * @throws IOException
     * @throws ComponentNotReadyException 
     */
    public void write(DataRecord record) throws IOException, ComponentNotReadyException {
    	if (!writeCommon()) {
    		return;
    	}
        
        // write the record according to value partition
        if (partitionKey == null) {
        	if (currentTarget == null) { // CLO-1634
        		prepareSingleTarget();
        	}
            // single formatter/getter
        	writeRecord2CurrentTarget(record);
        } else {
        	if (lookupTable != null) {
                // write the record according to lookup table
            	writeRecord4LookupTable(record);
            } else {
            	// just partition key without lookup table
            	writeRecord2MultiTarget(record, record);
            }
        }
    }

    /**
     * Writes the data record according to value partition.
     * 
     * @param record
     * @throws IOException
     * @throws ComponentNotReadyException
     */
    private final void writeRecord2MultiTarget(DataRecord keyRecord, DataRecord record) throws IOException, ComponentNotReadyException {
    	if (!sortedInput) {
    		//all output files are kept open
    		writeRecord2MultiTargetUnsortedInput(keyRecord, record);
    	} else {
    		//output files are generated after each other
    		writeRecord2MultiTargetSortedInput(keyRecord, record);
    	}
    }

    /**
     * Writes the data record according to value partition.
     * 
     * @param inputRecord
     * @throws IOException
     * @throws ComponentNotReadyException
     */
    private final void writeRecord2MultiTarget(DataRecord keyRecord, CloverBuffer buffer) throws IOException, ComponentNotReadyException {
    	if (!sortedInput) {
    		//all output files are kept open
    		writeRecord2MultiTargetUnsortedInput(keyRecord, buffer);
    	} else {
    		//output files are generated after each other
    		writeRecord2MultiTargetSortedInput(keyRecord, buffer);
    	}
    }
    
    private final void writeRecord2MultiTargetUnsortedInputCommon(DataRecord keyRecord) throws IOException, ComponentNotReadyException {
    	String keyString = getKeyString(keyRecord);
    	if ((currentTarget = multiTarget.get(keyString)) == null) {
    		currentTarget = createNewTarget();
    		currentTarget.setFileTag(useNumberFileTag ? numberFileTag++ : keyString);
    		currentTarget.init();
    		multiTarget.put(keyString, currentTarget);
    	}
		currentFormatter = currentTarget.getFormatter();
    }

    private final void writeRecord2MultiTargetUnsortedInput(DataRecord keyRecord, DataRecord record) throws IOException, ComponentNotReadyException {
    	writeRecord2MultiTargetUnsortedInputCommon(keyRecord);
		writeRecord2CurrentTarget(record);
    }

    private final void writeRecord2MultiTargetUnsortedInput(DataRecord keyRecord, CloverBuffer buffer) throws IOException, ComponentNotReadyException {
    	writeRecord2MultiTargetUnsortedInputCommon(keyRecord);
		writeRecord2CurrentTarget(buffer);
    }

    private final void writeRecord2MultiTargetSortedInputCommon(DataRecord keyRecord) throws IOException, ComponentNotReadyException {
    	String keyString = getKeyString(keyRecord);
    	if (lastKeyForSortedInput == null || !lastKeyForSortedInput.equals(keyString)) {
    		//the key has been changed
    		lastKeyForSortedInput = keyString;
    		
    		//close the previous file if necessary
    		if (currentTarget != null) {
	    		currentTarget.finish();
	    		currentTarget.close();
    		}
    		
    		currentTarget = createNewTarget();
    		currentTarget.setFileTag(useNumberFileTag ? numberFileTag++ : keyString);
    		currentTarget.init();
    	}
		currentFormatter = currentTarget.getFormatter();
    }
    
    private final void writeRecord2MultiTargetSortedInput(DataRecord keyRecord, DataRecord record) throws IOException, ComponentNotReadyException {
    	writeRecord2MultiTargetSortedInputCommon(keyRecord);
		writeRecord2CurrentTarget(record);
    }

    private final void writeRecord2MultiTargetSortedInput(DataRecord keyRecord, CloverBuffer buffer) throws IOException, ComponentNotReadyException {
    	writeRecord2MultiTargetSortedInputCommon(keyRecord);
		writeRecord2CurrentTarget(buffer);
    }

    private String getKeyString(DataRecord record) throws ComponentNotReadyException {
    	if (iPartitionOutFields == null) preparePartitionOutFields();    	
    	StringBuffer sb = new StringBuffer();
    	for (int pos: iPartitionOutFields) {
    		sb.append(record.getField(pos).toString());    		
    	}
    	return sb.toString();
    }
    
    private void preparePartitionOutFields() throws ComponentNotReadyException {
    	DataRecordMetadata metadata;
    	if (lookupTable != null) {
    		metadata = lookupTable.getMetadata();
    	} else {
    		metadata = this.metadata;
    	}
    	if (partitionOutFields != null) {
        	iPartitionOutFields = new int[partitionOutFields.length];
        	int pos;
        	for (int i=0; i<iPartitionOutFields.length; i++) {
        		pos = metadata.getFieldPosition(partitionOutFields[i]);
        		if (pos == -1) throw new ComponentNotReadyException("Field name '" + partitionOutFields[i] + "' not found in partition object (lookup table or partition key)");
        		iPartitionOutFields[i] = pos;
        	}
    	} else if (partitionKey != null) {
    		if (lookupTable == null) iPartitionOutFields = partitionKey.getKeyFields();
    		else throw new ComponentNotReadyException("Output field names are not defined for lookup table: " + lookupTable.getId());
    	}
    }
    
    private void setUnassignedTarget() throws IOException, ComponentNotReadyException {
		// creates new unassigned target if not exists
		if (unassignedTarget == null) {
			if (unassignedFileURL == null) return;
			unassignedTarget = createNewTarget();
			unassignedTarget.setFileName(unassignedFileURL);
			unassignedTarget.init();
		}
		currentTarget = unassignedTarget;
		currentFormatter = currentTarget.getFormatter();
    }
    
    /**
     * Writes the data record according to lookup table.
     * 
     * @param record
     * @throws IOException
     * @throws ComponentNotReadyException
     */
    private final void writeRecord4LookupTable(DataRecord record) throws IOException, ComponentNotReadyException {
//    	DataRecord keyRecord = lookupTable.get(new HashKey(partitionKey, record));
    	lookup.seek(record);
    	DataRecord keyRecord;
    	
    	if (!lookup.hasNext()) {
    		setUnassignedTarget();
    		writeRecord2CurrentTarget(record);
    		return;
    	}
    	
		// data filtering
    	do  {
    		keyRecord = lookup.next();
			writeRecord2MultiTarget(keyRecord, record);
			
			// get next record from database with the same key
			if (lookup.hasNext()) {
		        checkAndSetNextOutput();
			}else{
				return;
			}
    	}while (true);
    }
    
    /**
     * Writes the data record according to lookup table.
     * 
     * @param record
     * @param buffer
     * @throws IOException
     * @throws ComponentNotReadyException
     */
    private final void writeRecord4LookupTable(DataRecord record, CloverBuffer buffer) throws IOException, ComponentNotReadyException {
//    	DataRecord keyRecord = lookupTable.get(new HashKey(partitionKey, record));
    	lookup.seek(record);
    	DataRecord keyRecord;
    	
    	if (!lookup.hasNext()) {
    		setUnassignedTarget();
    		writeRecord2CurrentTarget(buffer);
    		return;
    	}
    	
		// data filtering
    	do  {
    		keyRecord = lookup.next();
			writeRecord2MultiTarget(keyRecord, buffer);
			
			// get next record from database with the same key
			if (lookup.hasNext()) {
		        checkAndSetNextOutput();
			}else{
				return;
			}
    	}while (true);
    }

    /**
     * Sets next output if records or bytes for the output are exceeded. 
     * 
     * @throws IOException
     */
    private final void checkAndSetNextOutput() throws IOException {
        if ((recordsPerFile > 0 && currentTarget.getRecords() >= recordsPerFile)
                || (bytesPerFile > 0 && currentTarget.getBytes() >= bytesPerFile)) {
        	currentTarget.setNextOutput();
        }
    }
    
    /**
     * Increments the record counters by 1
     * and the byte counter by the given number of bytes.
     * 
     * @param bytes - the number of bytes written to the output as the current record
     */
    private void incrementCounters(int bytes) {
    	currentTarget.setBytes(currentTarget.getBytes() + bytes);
    	currentTarget.setRecords(currentTarget.getRecords() + 1);
        counter++;
    }
    
    /**
     * Writes data into formatter and sets byte and record counters.
     * 
     * @param buffer
     * @throws IOException
     */
    private final void writeRecord2CurrentTarget(CloverBuffer buffer) throws IOException {
    	if (currentFormatter.isDirect()) {
    		int size = currentFormatter.writeDirect(buffer);
    		incrementCounters(size);
    	} else {
    		writeRecord2CurrentTarget(getCurrentRecord(buffer));
    	}
    }

    /**
     * Writes data into formatter and sets byte and record counters.
     * 
     * @param record
     * @throws IOException
     */
    private final void writeRecord2CurrentTarget(DataRecord record) throws IOException {
    	try {
    		int size = currentFormatter.write(record);
    		incrementCounters(size);
    	} catch (RuntimeException e) {
    		if (e.getCause() instanceof CharacterCodingException) {
    			throw new IOException("Converting exception in the record: " + counter + ". ", e);
    		} else {
    			throw e;
    		}
    	}
    }
    
    /**
     * Closes underlying formatter.
     * @throws IOException 
     */
    public void close() throws IOException {
    	if (outputClosed) return;
    	if (reset) {
    		try {
				prepareTargets();						// prepare output targets //TODO remove in 2.9, there is pre_execute
			} catch (ComponentNotReadyException e) {
				e.printStackTrace();
			}
    		reset = false;
    	}
    	if (multiTarget != null && !sortedInput) {
        	for (Entry<Object, TargetFile> entry: multiTarget.entrySet()) {
        		entry.getValue().close();
        	}
    	} else if (currentTarget != null) {
    		currentTarget.close();
    	}
    	if (unassignedTarget != null) {
    		unassignedTarget.close();
    		unassignedTarget = null;
    	}
    	outputClosed = true;
    	record = null;
    }
    
    public void finish() throws IOException{
    	if (reset) {
    		try {
				prepareTargets();						// prepare output targets //TODO remove in 2.9, there is pre_execute
			} catch (ComponentNotReadyException e) {
				e.printStackTrace();
			}
    		reset = false;
    	}
    	if (multiTarget != null && !sortedInput) {
        	for (Entry<Object, TargetFile> entry: multiTarget.entrySet()) {
        		entry.getValue().finish();
        	}
    	} else {
    		if (currentTarget != null) {
                currentTarget.finish();
    		}
    	}
    	if (unassignedTarget != null) {
    		unassignedTarget.finish();
    		// well, unassignedTarget.finish() closes its formatter (who closes its output channel) which renders unassignedTarget unusable
    		unassignedTarget.close();  // just to be sure
    		unassignedTarget = null; 
    	}
    }

    /**
     * Sets number of bytes written into separate file.
     * @param bytesPerFile
     */
    public void setBytesPerFile(int bytesPerFile) {
        this.bytesPerFile = bytesPerFile;
    }

    /**
     * Sets number of records written into separate file.
     * @param recordsPerFile
     */
    public void setRecordsPerFile(int recordsPerFile) {
        this.recordsPerFile = recordsPerFile;
    }
    
    
    public void setLogger(Log logger) {
    	TargetFile.setLogger(logger);
    }

    public void setAppendData(boolean appendData) {
        this.appendData = appendData;
    }

    /**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = skip;
        this.skipRecords = skip;
    }

    /**
     * Sets number of read reacords
     * @param numRecords
     */
    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

	public boolean isUseChannel() {
		return useChannel;
	}

	public void setUseChannel(boolean useChannel) {
		this.useChannel = useChannel;
	}

	/**
	 * 
	 * 
	 * @param partitionKeyNames
	 */
	public void setPartitionKeyNames(String[] partitionKeyNames) {
		this.partitionKeyNames = partitionKeyNames;
	}

	/**
	 * Gets partition key names used for partition key.
	 * 
	 * @return partitionKeyNames
	 */
	public String[] getPartitionKeyNames() {
		return partitionKeyNames;
	}

	/**
	 * Sets record key used for partition.
	 * 
	 * @param partitionKey
	 */
	public void setPartitionKey(RecordKey partitionKey) {
		this.partitionKey = partitionKey;
	}

	/**
	 * Gets record key used for partition.
	 * 
	 * @return RecordKey
	 */
	public RecordKey getPartitionKey() {
		return partitionKey;
	}
	
	/**
	 * Sets lookup table.
	 * 
	 * @param lookupTable - LookupTable
	 */
	public void setLookupTable(LookupTable lookupTable) {
		this.lookupTable = lookupTable;
	}

	/**
	 * Gets lookup table.
	 * 
	 * @return LookupTable
	 */
	public LookupTable getLookupTable() {
		return lookupTable;
	}
	
	/**
	 * Gets partition file tag type
	 * 
	 * @return PartitionFileTagType
	 */
	public boolean getPartitionFileTag() {
		return useNumberFileTag;
	}

	/**
	 * Sets partition file tag type
	 *
	 * @param partitionFileTagType - PartitionFileTagType
	 */
	public void setPartitionFileTag(PartitionFileTagType partitionFileTagType) {
		this.useNumberFileTag = partitionFileTagType == PartitionFileTagType.NUMBER_FILE_TAG;
	}

	/**
	 * Sets field names for lookup table.
	 * 
	 * @param partitionOutFields
	 */
	public void setPartitionOutFields(String[] partitionOutFields) {
		this.partitionOutFields = partitionOutFields;
	}

	/**
	 * Sets partition unassigned file name.
	 * 
	 * @param partitionUnassignedFileName
	 */
	public void setPartitionUnassignedFileName(String partitionUnassignedFileName) {
		this.unassignedFileURL = partitionUnassignedFileName;
	}

	/**
	 * Sets writable byte channel iterator.
	 * 
	 * @param channels
	 */
	public void setChannels(Iterator<WritableByteChannel> channels) {
		this.channels = channels;
	}	
	
    /**
     * Sets an output port for data writing to output record.
     * 
     * @param metadata
     */
    public void setOutputPort(OutputPort outputPort) {
    	this.outputPort = outputPort;
    }

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

	public int getCompressLevel() {
		return compressLevel;
	}

	public void setCompressLevel(int compressLevel) {
		this.compressLevel = compressLevel;
	}

	public void setMkDir(boolean mkDir) {
		this.mkDir = mkDir;
	}

	public void setStoreRawData(boolean storeRawData) {
		this.storeRawData = storeRawData; 
	}
	
	public void setSortedInput(boolean sortedInput) {
		this.sortedInput = sortedInput;
	}
	
	public void setCreateEmptyFiles(boolean createEmptyFiles) {
		this.createEmptyFiles = createEmptyFiles;
	}

	/**
	 * @return Count of records written to the current target.
	 */
	public int getCountOfRecordsAtCurrentTarget() {
		if (currentTarget != null) {
			return currentTarget.getRecords();
		}
		return -1;
	}
}
