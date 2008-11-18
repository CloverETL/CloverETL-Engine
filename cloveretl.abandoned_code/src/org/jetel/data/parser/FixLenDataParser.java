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
// FILE: c:/projects/jetel/org/jetel/data/FixLenDataParser.java

package org.jetel.data.parser;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Parsing fix length data.  It should be used in cases where new lines 
 *  do not separate records.  Supports fields up to <tt><b>FIELD_BUFFER_LENGTH</b></tt> long.
 *  Size of each individual field must be specified - through metadata definition.
 *
 *  This class is using the new IO (NIO) features introduced in Java 1.4 - directly mapped
 *  byte buffers & character encoders/decoders.
 * 
 * @author     David Pavlis
 * @since    August 21, 2002
 * @see        Parser
 * @see      Defaults
 * @revision    $Revision$
 */
public class FixLenDataParser implements Parser {

	private IParserExceptionHandler exceptionHandler;
	private ByteBuffer dataBuffer;
	private ByteBuffer fieldBuffer;
	//private CharBuffer fieldStringBuffer;  //not used
	private DataRecordMetadata metadata;

	private ReadableByteChannel reader;
	private CharsetDecoder decoder;
	private int recordCounter;
	private int fieldLengths[];

	static Log logger = LogFactory.getLog(FixLenDataParser.class);
	
	// Attributes

	/**
	 *Constructor for the FixLenDataParser object
	 *
	 * @since    August 21, 2002
	 */
	public FixLenDataParser() {
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);

		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
	}


	/**
	 *Constructor for the FixLenDataParser object
	 *
	 * @param  charsetDecoder  Description of Parameter
	 * @since                  August 21, 2002
	 */
	public FixLenDataParser(String charsetDecoder) {
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);

		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		decoder = Charset.forName(charsetDecoder).newDecoder();
	}


	/**
	 *  Gets the Next attribute of the FixLenDataParser object
	 *
	 * @return                  The Next value
	 * @exception  IOException  Description of Exception
	 * @since                   August 21, 2002
	 */
	public DataRecord getNext() throws JetelException {
		// create a new data record
		DataRecord record = new DataRecord(metadata);

		record.init();

		record = parseNext(record);
		if(exceptionHandler != null ) {  //use handler only if configured
			while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
		return record;
	}


	/**
	 *  Gets the Next attribute of the FixLenDataParser object
	 *
	 * @param  record           Description of Parameter
	 * @return                  The Next value
	 * @exception  IOException  Description of Exception
	 * @since                   August 21, 2002
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
		record = parseNext(record);
		if(exceptionHandler != null ) {  //use handler only if configured
			while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
		return record;
	}

	/**
	 *  Description of the Method
	 *
	 * @param  in         Description of Parameter
	 * @param  _metadata  Description of Parameter
	 * @since             August 21, 2002
	 */
	public void open(Object in, DataRecordMetadata _metadata) {
		CoderResult result;
		this.metadata = _metadata;

		reader = Channels.newChannel((InputStream)in);

		// create array of field sizes & initialize them
		fieldLengths = new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldLengths[i] = metadata.getField(i).getSize();
		}
		decoder.reset();
		// reset CharsetDecoder
		recordCounter = 1;
		// reset record counter
		dataBuffer.clear();
		dataBuffer.compact();
	}


	/**
	 *  Description of the Method
	 *
	 * @since    August 21, 2002
	 */
	public void close() {
		if (reader != null) {
			try {
				reader.close();
			}
			catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}


	/**
	 *  Assembles error message when exception occures during parsing
	 *
	 * @param  exceptionMessage  message from exception getMessage() call
	 * @param  recNo             recordNumber
	 * @param  fieldNo           fieldNumber
	 * @return                   error message
	 * @since                    September 19, 2002
	 */
	private String getErrorMessage(String exceptionMessage, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		return message.toString();
	}

	/**
	 *  Assembles error message when exception occures during parsing
	 *
	 * @param  exceptionMessage  message from exception getMessage() call
	 * @param  recNo             recordNumber
	 * @param  fieldNo           fieldNumber
	 * @return                   error message
	 * @since                    September 19, 2002
	 */
	private String getErrorMessage(String exceptionMessage,CharSequence  value, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #").append(recordCounter);
		message.append(" field ").append(metadata.getField(fieldNo).getName());
		message.append(" value \"").append(value).append("\"");
		return message.toString();
	}

	/**
	 *  Description of the Method
	 *
	 * @param  fielddBuffer     Description of Parameter
	 * @param  length           Description of Parameter
	 * @return                  Description of the Returned Value
	 * @exception  IOException  Description of Exception
	 * @since                   August 21, 2002
	 */
	private boolean readData(ByteBuffer fielddBuffer, int length) throws IOException {
		int size;
		// check if we have enough data in buffer to satisfy reading
		if (dataBuffer.remaining() < length) {
			dataBuffer.compact();
			size = reader.read(dataBuffer);
			if (logger.isDebugEnabled()) {
				logger.debug( "Read: " + size);
			}
			dataBuffer.flip();

			// if no more data or incomplete record
			if ((size == -1) || (dataBuffer.remaining() < length)) {
				return false;
			}
		}
		int saveLimit=dataBuffer.limit();
		dataBuffer.limit(dataBuffer.position()+length);
		fieldBuffer.put(dataBuffer);
		dataBuffer.limit(saveLimit);
		return true;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  record           Description of Parameter
	 * @return                  Description of the Returned Value
	 * @exception  IOException  Description of Exception
	 * @since                   August 21, 2002
	 */
	private DataRecord parseNext(DataRecord record) throws JetelException {
		int fieldCounter = 0;
		int character;

		// populate all data fields

		while (fieldCounter < metadata.getNumFields()) {
			// we clear our buffers
			fieldBuffer.clear();
			//fieldStringBuffer.clear();

			// populate fieldBuffer with data
			try {
				if (!readData(fieldBuffer, fieldLengths[fieldCounter])) {
					reader.close();
					fieldBuffer.flip();
					if ((fieldBuffer.remaining()>0)||(fieldCounter>0)){
						throw new RuntimeException(getErrorMessage("Incomplete record", recordCounter, fieldCounter));
					}else{
						return null;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new JetelException(e.getMessage());
			}
			
			// prepare for populating data field
			fieldBuffer.flip();
			populateField(record, fieldCounter, fieldBuffer);
			fieldCounter++;
		}
		recordCounter++;
		return record;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  record    Description of Parameter
	 * @param  fieldNum  Description of Parameter
	 * @param  data      Description of Parameter
	 * @since            August 21, 2002
	 */
	private void populateField(DataRecord record, int fieldNum, ByteBuffer data) {
		try {
			record.getField(fieldNum).fromByteBuffer(data,decoder);
		} catch (BadDataFormatException bdfe) {
			if(exceptionHandler != null ) {  //use handler only if configured
                exceptionHandler.populateHandler(getErrorMessage(bdfe.getMessage(), recordCounter, fieldNum), record, -1, fieldNum, data.toString(), bdfe);
			} else {
				throw new RuntimeException(getErrorMessage(bdfe.getMessage(), recordCounter, fieldNum));
			}
		}
		catch (Exception ex) {
			throw new RuntimeException(getErrorMessage(ex.getMessage(), recordCounter, fieldNum));
		}
	}

    public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }


    public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }


    public PolicyType getPolicyType() {
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
    }


	public int skip(int nRec) throws JetelException {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	public void setDataSource(Object inputDataSource) {
		throw new UnsupportedOperationException();
	}

}
/*
 *  end class FixLenDataParser
 */

