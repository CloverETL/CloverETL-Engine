/*
 * jETeL/Clover.ETL - Java based ETL application framework. Copyright (C)
 * 2002-2004 David Pavlis <david_pavlis@hotmail.com>
 * 
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

package org.jetel.database.dbf;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.jetel.data.parser.Parser;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.Parser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author DPavlis
 * @since 29.6.2004
 * 
 * TODO To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Generation - Code and Comments
 */

public class DBFDataParser implements Parser {

    private IParserExceptionHandler exceptionHandler;

    private String charSet;

    private DBFAnalyzer dbfAnalyzer;

    private FileChannel dbfFile;

    private DataRecordMetadata metadata;

    private CharBuffer charBuffer;

    private ByteBuffer buffer;

    private int recordCounter;

    private int totalRecords;

    private CharsetDecoder decoder;

    private int fieldSizes[];

    public DBFDataParser() {
    }

    public DBFDataParser(String charSet) {
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
    public void close() {
        try {
            dbfFile.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /*
     * Reads next record from datafile
     * 
     * @see org.jetel.data.DataParser#getNext()
     */
    public DataRecord getNext() throws JetelException {
        // create a new data record
        DataRecord record = new DataRecord(metadata);
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
        if (recordCounter >= totalRecords) { return null; }
        try {
            if (!populateCharBuffer()) { throw new JetelException(
                    "Data error - incomplete record read !"); }
        } catch (IOException e) {
            throw new JetelException(e.getMessage());
        }
        // populate all data fields
        while (fieldCounter < metadata.getNumFields()) {
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
            record.getField(fieldNum).fromString(data.toString());
        } catch (BadDataFormatException bdfe) {
            if (exceptionHandler != null) { //use handler only if configured
                exceptionHandler.populateHandler(getErrorMessage(bdfe.getMessage(),
                        data, recordCounter, fieldNum), record, -1, fieldNum, data.toString(), bdfe);
            } else {
                throw new RuntimeException(getErrorMessage(bdfe.getMessage(),
                        data, recordCounter, fieldNum));
            }
        } catch (Exception ex) {
            throw new RuntimeException(getErrorMessage(ex.getMessage(), data,
                    recordCounter, fieldNum));
        }
    }

    
    /*
     * Opens specified datafile. Reads all available metadata from header.
     * This method must be called prior to calling getNext().
     * 
     * @see org.jetel.data.DataParser#open(java.lang.Object,
     *      org.jetel.metadata.DataRecordMetadata)
     */
    public void open(Object inputDataSource, DataRecordMetadata _metadata)
            throws ComponentNotReadyException {
        
        if (! (inputDataSource instanceof FileInputStream) ){
            throw new ComponentNotReadyException("Invalid input data object passed - isn't an InputStream");
        }
        
        dbfFile = ((FileInputStream)inputDataSource).getChannel();
        
        
        metadata = _metadata;
        dbfAnalyzer = new DBFAnalyzer();
        try {
            dbfAnalyzer.analyze(dbfFile, metadata.getName());
        } catch (Exception ex) {
            throw new ComponentNotReadyException(ex.getMessage());
        }
        //set-up buffers
        charBuffer = CharBuffer.allocate(dbfAnalyzer.getRecSize());
        buffer = ByteBuffer
                .allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
        if (charSet != null) {
            decoder = Charset.forName(charSet).newDecoder();
        } else {
            decoder = Charset.forName(
                    DBFTypes.dbfCodepage2Java(dbfAnalyzer.getDBFCodePage()))
                    .newDecoder();
        }
        decoder.reset();
        totalRecords = dbfAnalyzer.getNumRows();
        try {
            dbfFile.position(dbfAnalyzer.getDBFDataOffset());
        } catch (IOException ex) {
            throw new ComponentNotReadyException(
                    "Error when setting initial reading position: "
                            + ex.getMessage());
        }
        // initialize array of fields sizes
        fieldSizes = new int[metadata.getNumFields()];
        DBFFieldMetadata[] fieldMetadata = dbfAnalyzer.getFields();
        for (int i = 0; i < fieldSizes.length; i++) {
            fieldSizes[i] = metadata.getField(i).getSize();
        }
        buffer.flip(); // no data initially
        recordCounter = 0;
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
        CoderResult decodingResult;
        charBuffer.clear();
        decodingResult = decoder.decode(buffer, charBuffer, false);
        // the whole record should be read at once
        if (charBuffer.position() < charBuffer.limit()) {
            // we need to read more - buffer is not completely
            // filled
            buffer.clear();
            size = dbfFile.read(buffer);
            buffer.flip();
            // if no more data, return -1
            if (size == -1) { return false; }
            decodingResult = decoder.decode(buffer, charBuffer, false);
            if (charBuffer.position() < charBuffer.limit()) { return false; // still
                                                                            // not
                                                                            // enough
                                                                            // data
                                                                            // -
                                                                            // some
                                                                            // problem
            }
        }
        charBuffer.flip();
        return true;
    }

    /**
     * Reads chars from right and removes all binary zeros - 0x0000. It finishes
     * when finds first valid/non-zero character or reaches the begin of the
     * buffer
     * 
     * @param buffer
     */
    private void removeBinaryZeros(CharBuffer buffer) {
        for (int i = buffer.limit() - 1; i >= buffer.position(); i--) {
            if (buffer.charAt(i) == (char) 0) {
                buffer.put(i, ' ');
            } else {
                return;
            }
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
    private String getErrorMessage(String exceptionMessage, CharSequence value,
            int recNo, int fieldNo) {
        StringBuffer message = new StringBuffer();
        message.append(exceptionMessage);
        message.append(" when parsing record #").append(recordCounter);
        message.append(" field ").append(metadata.getField(fieldNo).getName());
        message.append(" value \"").append(value).append("\"");
        return message.toString();
    }

    public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }

    public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

	public int skip(int nRec) {
		throw new UnsupportedOperationException("Not yet implemented");
	}
}