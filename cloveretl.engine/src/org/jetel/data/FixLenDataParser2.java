/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
// FILE: c:/projects/jetel/org/jetel/data/FixLenDataParser2.java

package org.jetel.data;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Parsing fix length data. Supports fields up to <tt><b>FIELD_BUFFER_LENGTH</b></tt> long.
 *  Size of each individual field must be specified - through metadata definition.
 *
 *  This class is using the new IO (NIO) features introduced in Java 1.4 - directly mapped
 *  byte buffers & character encoders/decoders
 *
 * @author     David Pavlis,Wes Maciorowski     
 * @since    August 21, 2002
 * @see        DataParser
 * @see      Defaults
 * @revision    $Revision$
 */
public class FixLenDataParser2 implements DataParser {
	private BadDataFormatExceptionHandler handlerBDFE;
	private ByteBuffer dataBuffer;
	private ByteBuffer fieldBuffer;
	private DataRecordMetadata metadata;
	private int recordCounter;
	private int fieldLengths[];
	private int recordLength;
	private BufferedReader reader = null;
	private CharsetDecoder decoder;
	/**
	 *Constructor for the FixLenDataParser object
	 *
	 * @since    August 21, 2002
	 */
	public FixLenDataParser2() {
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
	}
	/**
	 *Constructor for the FixLenDataParser object
	 *
	 * @param  charsetDecoder  Description of Parameter
	 * @since    August 21, 2002
	 */
	public FixLenDataParser2(String charsetDecoder) {
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		//fieldStringBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		decoder = Charset.forName(charsetDecoder).newDecoder();
	}
	/**
	 *  An operation that opens/initializes parser.
	 *
	 *@param  in         InputStream of delimited text data
	 *@param  _metadata  Metadata describing the structure of data
	 *@since             March 27, 2002
	 */
	public void open(InputStream in, DataRecordMetadata _metadata) {
		CoderResult result;
		DataFieldMetadata fieldMetadata;
		this.metadata = _metadata;
        reader = 
			new BufferedReader(Channels.newReader( ((FileInputStream) in).getChannel(), Defaults.DataParser.DEFAULT_CHARSET_DECODER ) );
		// create array of field sizes & initialize them
		fieldLengths = new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldLengths[i] = metadata.getField(i).getSize();
			recordLength = recordLength + fieldLengths[i];
		}
		decoder.reset();
		// reset CharsetDecoder
		recordCounter = 0;
		// reset record counter
	}
	/**
	 *  An operation that does ...
	 *
	 *@param  record           Description of Parameter
	 *@return                  Next DataRecord (parsed from input data) or null if
	 *      no more records available
	 *@exception  IOException  Description of Exception
	 *@since                   March 27, 2002
	 */
	private DataRecord parseNext(DataRecord record) throws IOException {
		int fieldCounter = 0;
		int posCounter = 0;
		String line = null;
		line = reader.readLine();
		while(line != null && line.trim().equalsIgnoreCase("")) {	//skip blank lines
			line = reader.readLine();
		}
		if((line) == null) {  //end of file been reached
			reader.close();
			return null;
		}
		if(line.length()<recordLength){ //incomplete record
			//- incomplete record - do something
			throw new RuntimeException("Incomplete record");
			//TODO - need exception records bucket
		}
		// process the line 
		// populate all data fields
		try {
			while (fieldCounter < metadata.getNumFields()) {
				try {
				populateField(record, fieldCounter, line.substring(posCounter,posCounter+fieldLengths[fieldCounter]).trim());
				} catch (BadDataFormatException ex) {
					handlerBDFE.populateFieldFailure(record, fieldCounter, line.substring(posCounter,posCounter+fieldLengths[fieldCounter]).trim());
				}
				posCounter += fieldLengths[fieldCounter];
				fieldCounter++;
			}
		} catch (Exception ex) {

			throw new RuntimeException(ex.getMessage());
		}

		return record;
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
	 *  Returs next data record parsed from input stream or NULL if no more data
	 *  available The specified DataRecord's fields are altered to contain new
	 *  values.
	 *
	 *@param  record           Description of Parameter
	 *@return                  The Next value
	 *@exception  IOException  Description of Exception
	 *@since                   May 2, 2002
	 */
	public DataRecord getNext(DataRecord record) throws IOException {
		record = parseNext(record);
		while(handlerBDFE.isThrowException()) {
			handlerBDFE.handleException(record);
			record.init();
			record = parseNext(record);
		}
		return record;
	}
	/**
	 *  Description of the Method
	 *
	 *@param  record    Description of Parameter
	 *@param  fieldNum  Description of Parameter
	 *@param  data      Description of Parameter
	 *@since            March 28, 2002
	 */
	protected void populateField(DataRecord record, int fieldNum, String data) {
		try {
			record.getField(fieldNum).fromString( data );

		} catch (BadDataFormatException bdfe) {
			handlerBDFE.populateFieldFailure(record,fieldNum,data);
		} catch (Exception ex) {
			throw new RuntimeException(getErrorMessage(ex.getMessage(), recordCounter, fieldNum));
		}
	}

	/**
	 *  Gets the Next attribute of the FixLenDataParser object
	 *
	 * @return                  The Next value
	 * @exception  IOException  Description of Exception
	 * @since                   August 21, 2002
	 */
	public DataRecord getNext() throws IOException {
		// create a new data record
		DataRecord record = new DataRecord(metadata);
		record.init();
		return parseNext(record);
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
	 * @param handler
	 */
	public void addBDFHandler(BadDataFormatExceptionHandler handler) {
		this.handlerBDFE = handler;
	}

}
