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
// FILE: c:/projects/jetel/org/jetel/data/FixLenDataParser.java

package org.jetel.data;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;
import java.io.*;

import org.jetel.exception.JetelException;
import org.jetel.metadata.*;

/**
 *  Parsing fix length data. Supports fields up to <tt><b>FIELD_BUFFER_LENGTH</b></tt> long.
 *  Size of each individual field must be specified - through metadata definition.
 *
 *  This class is using the new IO (NIO) features introduced in Java 1.4 - directly mapped
 *  byte buffers & character encoders/decoders
 *
 * @author     David Pavlis
 * @since    August 21, 2002
 * @see        DataParser
 * @see      Defaults
 * @revision    $Revision$
 */
public class FixLenDataParser implements DataParser {

	private ByteBuffer dataBuffer;
	private ByteBuffer fieldBuffer;
	private CharBuffer fieldStringBuffer;
	private DataRecordMetadata metadata;

	private ReadableByteChannel reader;
	private CharsetDecoder decoder;
	private int recordCounter;
	private int fieldLengths[];

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

		try {
			return parseNext(record);
		} catch (IOException e) {
			e.printStackTrace();
			throw new JetelException(e.getMessage());
		}
	}


	/**
	 *  Gets the Next attribute of the FixLenDataParser object
	 *
	 * @param  record           Description of Parameter
	 * @return                  The Next value
	 * @exception  IOException  Description of Exception
	 * @since                   August 21, 2002
	 */
	public DataRecord getNext(DataRecord record) throws IOException {
		return parseNext(record);
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

		reader = ((FileInputStream) in).getChannel();

		// create array of field sizes & initialize them
		fieldLengths = new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldLengths[i] = metadata.getField(i).getSize();
		}
		decoder.reset();
		// reset CharsetDecoder
		recordCounter = 0;
		// reset record counter
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
			// if no more data or incomplete record
			if ((size == -1) || (dataBuffer.remaining() < length)) {
				return false;
			}
			dataBuffer.flip();
		}
		fieldBuffer.limit(length);
		fieldBuffer.put(dataBuffer);
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
	private DataRecord parseNext(DataRecord record) throws IOException {
		int fieldCounter = 0;
		int character;
		long size = 0;

		// populate all data fields

		while (fieldCounter < metadata.getNumFields()) {
			// we clear our buffers
			fieldBuffer.clear();
			fieldStringBuffer.clear();

			// populate fieldBuffer with data
			if (!readData(fieldBuffer, fieldLengths[fieldCounter])) {
				reader.close();
				fieldBuffer.flip();
				if ((fieldBuffer.remaining()>0)||(fieldCounter>0)){
					throw new RuntimeException(getErrorMessage("Incomplete record", recordCounter, fieldCounter));
				}else{
					return null;
				}
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
		}
		catch (Exception ex) {
			throw new RuntimeException(getErrorMessage(ex.getMessage(), recordCounter, fieldNum));
		}
	}

}
/*
 *  end class FixLenDataParser
 */

