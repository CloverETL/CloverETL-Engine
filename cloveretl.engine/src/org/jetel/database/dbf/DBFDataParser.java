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
package org.jetel.database.dbf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.parser.AbstractParser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author DPavlis
 * @since 29.6.2004
 * 
 * TODO To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Generation - Code and Comments
 */

public class DBFDataParser extends AbstractParser {

	private final static Log logger = LogFactory.getLog(DBFDataParser.class);

	private static final String METADATA_PROPERTY_CHARSET="charset";
    
    private IParserExceptionHandler exceptionHandler;

    private String charSet;

    private DBFAnalyzer dbfAnalyzer;

    private ReadableByteChannel dbfFile;

    private DataRecordMetadata metadata;

    private CharBuffer charBuffer;

    private byte[] record;

    private ByteBuffer buffer;

    private int recordCounter;

    private int totalRecords;

    private CharsetDecoder decoder;

    private int fieldSizes[];

    private DataFieldMetadata dataFieldMetadata; 
    
	private boolean[] isAutoFilling;

	private int bytesProcessed;

	private int read;

    public DBFDataParser(DataRecordMetadata metadata) {
    	this.metadata = metadata;
    }

    public DBFDataParser(DataRecordMetadata metadata, String charSet) {
    	this.metadata = metadata;
        this.charSet = charSet;
    }
    
    
    /**
     * Returns charset this parser has been created with
     * @return Charset of this parser or null if none was specified
     */
    public String getCharset() {
    	return(this.charSet);
    }

    /**
	 * Returns data policy type for this parser
	 * @return Data policy type or null if none was specified
	 */
	@Override
	public PolicyType getPolicyType() {
		if (this.exceptionHandler != null) {
			return this.exceptionHandler.getType();
		} else {
			return null;
		}
			
	}
        
    /*
     * closes the parser (opened file)
     * 
     * @see org.jetel.data.DataParser#close()
     */
    @Override
	public void close() {
        try {
            doReleaseDataSource();
        } catch (IOException ex) {
            logger.warn("Failed to close data source", ex);
        }
    }

    /*
     * Reads next record from datafile
     * 
     * @see org.jetel.data.DataParser#getNext()
     */
    @Override
	public DataRecord getNext() throws JetelException {
        // create a new data record
        DataRecord record = DataRecordFactory.newRecord(metadata);
        record = parseNext(record);
        if (exceptionHandler != null) { //use handler only if configured
            while (exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
                record = parseNext(record);
            }
        }
        return record;
    }

    /*
     * Reads next record from datafile
     * 
     * @see org.jetel.data.DataParser#getNext(org.jetel.data.DataRecord)
     */
    @Override
	public DataRecord getNext(DataRecord record) throws JetelException {
        record = parseNext(record);
        if (exceptionHandler != null) { //use handler only if configured
            while (exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
                record = parseNext(record);
            }
        }
        return record;
    }

    /*
     * Parses next record from data file (called by higher-level public methods)
     * 
     * @see org.jetel.data.DataParser#getNext(org.jetel.data.DataRecord)
     */
    private DataRecord parseNext(DataRecord record) throws JetelException {
        if (recordCounter >= totalRecords) { 
            bytesProcessed = dbfAnalyzer.getNumRows() * dbfAnalyzer.getRecSize();
        	return null; 
        }
        loadRecord();
        // populate all data fields
        ByteBuffer recordBuffer = ByteBuffer.wrap(this.record);
        recordBuffer.limit(0);
        for (int i = 0; i < metadata.getNumFields(); i++) {
        	recordBuffer.limit(recordBuffer.limit() + metadata.getField(i).getSize());
        	if (!isAutoFilling[i]) {
                charBuffer.clear();
				decoder.decode(recordBuffer, charBuffer, false);
                charBuffer.flip();
                populateField(record, i, charBuffer);
        	}
        }
        recordCounter++;
        return record;
    }

    /**
     * Populates particular field with data parsed from datafile
     * 
     * @param record
     * @param fieldNum
     * @param data
     */
    private void populateField(DataRecord record, int fieldNum, CharBuffer data) {
        try {
            //removeBinaryZeros(data);
        	dataFieldMetadata = metadata.getField(fieldNum);
            char fieldType=dataFieldMetadata.getType();
            if (fieldType==DataFieldMetadata.DATE_FIELD || fieldType==DataFieldMetadata.DATETIME_FIELD){
                if (StringUtils.isBlank(data)){
                    record.getField(fieldNum).setNull(true);
                    return;
                }
            }
            if (dataFieldMetadata.isTrim()) {
            	StringUtils.trim(data);
            }
            record.getField(fieldNum).fromString(data.toString());
        } catch (BadDataFormatException bdfe) {
        	bdfe.setRecordNumber(recordCounter);
            if (exceptionHandler != null) { //use handler only if configured
                exceptionHandler.populateHandler(getErrorMessage(
                        data, recordCounter, fieldNum), record, recordCounter, fieldNum, data.toString(), bdfe);
            } else {
                throw new RuntimeException(getErrorMessage(data, recordCounter, fieldNum), bdfe);
            }
        } catch (Exception ex) {
            throw new RuntimeException(getErrorMessage(data, recordCounter, fieldNum), ex);
        }
    }

    
    /*
     * Opens specified datafile. Reads all available metadata from header.
     * This method must be called prior to calling getNext().
     * 
     * @see org.jetel.data.DataParser#open(java.lang.Object,
     *      org.jetel.metadata.DataRecordMetadata)
     */
    @Override
	public void init()
            throws ComponentNotReadyException {
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		isAutoFilling = new boolean[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			isAutoFilling[i] = metadata.getField(i).getAutoFilling() != null;
		}
    }
    
    private void doReleaseDataSource() throws IOException {
        if (dbfFile != null && dbfFile.isOpen()) {
        	dbfFile.close();
        }
    }

	@Override
	protected void releaseDataSource() {
        try {
            doReleaseDataSource();
        } catch (IOException ex) {
            logger.warn("Failed to release data source", ex);
        }
	}

    /* (non-Javadoc)
     * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
     */
    @Override
	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
        if (releaseDataSource) close();
        
        if (inputDataSource instanceof FileInputStream){
            dbfFile = ((FileInputStream)inputDataSource).getChannel();
        }else if (inputDataSource instanceof FileChannel){
            dbfFile = (FileChannel)inputDataSource;
		} else if (inputDataSource instanceof ReadableByteChannel) {
			dbfFile = ((ReadableByteChannel) inputDataSource);
        } else if (inputDataSource instanceof InputStream) {
        	dbfFile = Channels.newChannel((InputStream) inputDataSource);
        } else{
            throw new RuntimeException("Invalid input data object passed - isn't an InputStream or FileChannel");
        }
        
        dbfAnalyzer = new DBFAnalyzer();
        read = 0;
		bytesProcessed = 0;
        try {
        	read = dbfAnalyzer.analyze(dbfFile, metadata.getName());
        } catch (Exception ex) {
            throw new ComponentNotReadyException(ex);
        }
        //set-up buffers
        charBuffer = CharBuffer.allocate(dbfAnalyzer.getRecSize());
        record = new byte[dbfAnalyzer.getRecSize()];
        buffer = ByteBuffer
                .allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
        
       if (charSet == null) {
            charSet = metadata.getProperty(METADATA_PROPERTY_CHARSET);
            if (charSet == null) {
                charSet = DBFTypes.dbfCodepage2Java(dbfAnalyzer
                        .getDBFCodePage());
            }
        }
        decoder = Charset.forName(charSet).newDecoder();
        decoder.reset();
        totalRecords = dbfAnalyzer.getNumRows();
        try {
            //dbfFile.position(dbfAnalyzer.getDBFDataOffset());
            dbfFile.read(ByteBuffer.allocate(dbfAnalyzer.getDBFDataOffset()-read));
        } catch (IOException ex) {
            throw new ComponentNotReadyException("Error when setting initial reading position", ex);
        }
        // initialize array of fields sizes
        fieldSizes = new int[metadata.getNumFields()];
        for (int i = 0; i < fieldSizes.length; i++) {
            fieldSizes[i] = metadata.getField(i).getSize();
        }
        buffer.flip(); // no data initially
        recordCounter = 0;
    }

	/**
	 * Discard bytes for incremental reading.
	 * 
	 * @param bytes
	 * @throws IOException 
	 */
	private void discardBytes(int bytes) throws IOException {
		totalRecords -= bytes/dbfAnalyzer.getRecSize();
		if (dbfFile instanceof FileChannel) {
			((FileChannel)dbfFile).position(bytes+read);
			return;
		}
		while (bytes > 0) {
			buffer.clear();
			if (bytes < Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE) buffer.limit(bytes);
			try {
				dbfFile.read(buffer);
			} catch (IOException e) {
				break;
			}
			bytes -= Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE;
		}
		buffer.clear();
		buffer.flip();
	}
	
	private void loadRecord() throws JetelException {
		
		try {
			if (buffer.remaining() < dbfAnalyzer.getRecSize()) {
				buffer.compact();
				int size = dbfFile.read(buffer);
        		buffer.flip();
        		if (size < 0) {
        			throw new JetelException("Data error - incomplete record read!! " +
    			            "Possible problem with encoding - " + StringUtils.quote(charSet) + " used for parsing"); 
        		}
			}
			buffer.get(record);
			
		} catch (IOException e) {
		    throw new JetelException("Failed to read record", e);
		}
	}

    /**
     * Assembles error message when exception occures during parsing
     * 
     * @param exceptionMessage
     *            message from exception getMessage() call
     * @param recNo
     *            recordNumber
     * @param fieldNo
     *            fieldNumber
     * @return error message
     * @since September 19, 2002
     */
    private String getErrorMessage(CharSequence value, int recNo, int fieldNo) {
        StringBuffer message = new StringBuffer();
        message.append("Error when parsing record #").append(recordCounter);
        message.append(" field ").append(metadata.getField(fieldNo).getName());
        message.append(" value \"").append(value).append("\"");
        return message.toString();
    }

    @Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }

    @Override
	public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

	@Override
	public int skip(int nRec) throws JetelException {
		for (int i = 0; i < nRec; i++) {
			// just read record data, no parsing performed
	        loadRecord();
	        recordCounter++;
		}
		
		return nRec;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#reset()
	 */
	@Override
	public void reset() {
		recordCounter = 0;
		bytesProcessed = 0;
	}

	@Override
	public Object getPosition() {
		return bytesProcessed;
	}

	@Override
	public void movePosition(Object position) throws IOException {
		int pos = 0;
		if (position instanceof Integer) {
			pos = ((Integer) position).intValue();
		} else if (position != null) {
			pos = Integer.parseInt(position.toString());
		}
		if (pos > 0) {
			discardBytes(pos);
			bytesProcessed = pos;
		}
	}

	@Override
    public void preExecute() throws ComponentNotReadyException {
    }
    
	@Override
    public void postExecute() throws ComponentNotReadyException {    	
		if (releaseDataSource) {
			releaseDataSource();
		}
    	reset();
    }
    
	@Override
    public void free() {
    	close();
    }

	@Override
	public boolean nextL3Source() {
		return false;
	}
	
}