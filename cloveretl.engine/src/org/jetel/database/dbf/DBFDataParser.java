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
        record.init();
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
        int fieldCounter = 0;
        int limit = 0;
        int position = 0;
        if (recordCounter >= totalRecords) { 
            bytesProcessed = dbfAnalyzer.getNumRows() * dbfAnalyzer.getRecSize();
        	return null; 
        }
        loadRecordIntoCharBuffer();
        // populate all data fields
        while (fieldCounter < metadata.getNumFields()) {
        	if (isAutoFilling[fieldCounter]) {
                fieldCounter++;
                continue;
        	}
            limit += fieldSizes[fieldCounter];
            charBuffer.limit(limit);
            charBuffer.position(position);
            populateField(record, fieldCounter, charBuffer);
            position = limit;
            fieldCounter++;
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
        //verify that metadata correspond to num of fields (plus 1 - deleted flag)
        int dbfFieldCount=0;
        for (int i=0; i<metadata.getNumFields(); i++) {
        	if (metadata.getField(i).getAutoFilling() == null) {
        		dbfFieldCount++;
        	}
        }
        
//        if (dbfFieldCount!=(dbfAnalyzer.getNumFields()+1)){
//            throw new ComponentNotReadyException("Invalid metadata - DBF file indicates different number of fields than metadata!"); 
//        }
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
	
    /**
     * Method which populates charbuffer with the whole record. Then individual
     * fields are extracted using their size & calculated offset
     * 
     * @return true if the whole record was read
     * @throws IOException
     */
    private boolean populateCharBuffer() throws IOException {
        int size;
        charBuffer.clear();
        decoder.decode(buffer, charBuffer, false);
        // the whole record should be read at once
        if (charBuffer.position() < charBuffer.limit()) {
            // we need to read more - buffer is not completely
            // filled
            buffer.clear();
            size = dbfFile.read(buffer);
            buffer.flip();
            // if no more data, return -1
            if (size == -1) return false;
            decoder.decode(buffer, charBuffer, false);
            if (charBuffer.position() < charBuffer.limit()) {
            	// still not enough data - some problem
            	return false;
            }
        }
        charBuffer.flip();
        return true;
    }

	/**
	 * Calls {@link #populateCharBuffer()} and throws exception if it fails.
	 * @throws JetelException if a complete record could not be read into charBuffer.
	 */
	private void loadRecordIntoCharBuffer() throws JetelException {
		try {
		    if (!populateCharBuffer()) {
		    	throw new JetelException("Data error - incomplete record read!! " +
		            "Possible problem with encoding - " + StringUtils.quote(charSet) + " used for parsing"); 
		    }
		} catch (IOException e) {
		    throw new JetelException("Failed to read record", e);
		}
	}

    /**
     * Reads chars from right and removes all binary zeros - 0x0000. It finishes
     * when finds first valid/non-zero character or reaches the begin of the
     * buffer
     * 
     * @param buffer
     */
    /* never used
    private void removeBinaryZeros(CharBuffer buffer) {
        for (int i = buffer.limit() - 1; i >= buffer.position(); i--) {
            if (buffer.charAt(i) == (char) 0) {
                buffer.put(i, ' ');
            } else {
                return;
            }
        }
    }
    */

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
    /* never used
    private String getErrorMessage(String exceptionMessage, int recNo,
            int fieldNo) {
        StringBuffer message = new StringBuffer();
        message.append(exceptionMessage);
        message.append(" when parsing record #");
        message.append(recordCounter);
        message.append(" field ");
        message.append(metadata.getField(fieldNo).getName());
        return message.toString();
    }
    */

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
	        loadRecordIntoCharBuffer();
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