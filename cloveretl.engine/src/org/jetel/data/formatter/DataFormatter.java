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

// FILE: c:/projects/jetel/org/jetel/data/DelimitedDataFormatter.java

package org.jetel.data.formatter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Outputs common data record. Handles encoding of characters. Uses WriteableChannel.
 *
 * @author     Martin Zatopek, OpenTech s.r.o (www.opentech.cz)
 * @since      26. 9. 2005
 * @see        Formatter
 */
public class DataFormatter implements Formatter {
	private String charSet = null;
	private ByteBuffer fieldBuffer;
	private ByteBuffer fieldFiller;
	private DataRecordMetadata metadata;
	private WritableByteChannel writer;
	private CharsetEncoder encoder;
    private byte[][] delimiters;
	private int delimiterLength[];
	private int fieldLengths[];
	private ByteBuffer dataBuffer;

	// use space (' ') to fill/pad field
	private final static char DEFAULT_FILLER_CHAR = ' ';

	// Associations

	// Operations
	
	public DataFormatter(){
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
		encoder = Charset.forName(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER).newEncoder();
		initFieldFiller();
		encoder.reset();
	}
	
	public DataFormatter(String charEncoder){
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
		charSet = charEncoder;
		encoder = Charset.forName(charEncoder).newEncoder();
		initFieldFiller();
		encoder.reset();
	}
	
	/**
	 *  Description of the Method
	 *
	 * @param  out        Description of Parameter
	 * @param  _metadata  Description of Parameter
	 * @since             March 28, 2002
	 */
	public void open(Object out, DataRecordMetadata _metadata) {
		this.metadata = _metadata;

		// create buffered output stream reader 
		writer = (WritableByteChannel) out;

		// create array of delimiters & initialize them
		// create array of field sizes & initialize them
		delimiters = new byte[metadata.getNumFields()][];
        delimiterLength = new int[metadata.getNumFields()];
		fieldLengths = new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			if(metadata.getField(i).isDelimited()) {
                delimiters[i] = (metadata.getField(i).getDelimiters()[0]).getBytes();
				delimiterLength[i] = delimiters[i].length;
			} else {
				fieldLengths[i] = metadata.getField(i).getSize();
			}
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @since    March 28, 2002
	 */
	public void close() {
		try{
			flush();
			writer.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}


	/**
	 *  Description of the Method
	 * @throws IOException
	 *
	 * @since    March 28, 2002
	 */
	public void flush() throws IOException {
		dataBuffer.flip();
		writer.write(dataBuffer);
		dataBuffer.clear();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  record           Description of Parameter
	 * @exception  IOException  Description of Exception
	 * @since                   March 28, 2002
	 */
	public void write(DataRecord record) throws IOException {
		int size;
		for (int i = 0; i < metadata.getNumFields(); i++) {
			if(metadata.getField(i).isDelimited()) {
				fieldBuffer.clear();
				record.getField(i).toByteBuffer(fieldBuffer, encoder);
				if((fieldBuffer.position() + delimiterLength[i]) > dataBuffer.remaining()) {
					flush();
				}
				fieldBuffer.flip();
				dataBuffer.put(fieldBuffer);
				dataBuffer.put(delimiters[i]);
			} else { //fixlen field
				if (fieldLengths[i] > dataBuffer.remaining()) {
					flush();
				}
				fieldBuffer.clear();
				record.getField(i).toByteBuffer(fieldBuffer, encoder);
				size = fieldBuffer.position();
				if (size < fieldLengths[i]) {
					fieldFiller.rewind();
					fieldFiller.limit(fieldLengths[i] - size);
					fieldBuffer.put(fieldFiller);
				}
				fieldBuffer.flip();
				fieldBuffer.limit(fieldLengths[i]);
				dataBuffer.put(fieldBuffer);
			}
		}
	}
	
	/**
	 * Returns name of charset which is used by this formatter
	 * @return Name of charset or null if none was specified
	 */
	public String getCharsetName() {
		return(this.charSet);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#setOneRecordPerLinePolicy(boolean)
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 *  Initialization of the FieldFiller buffer
	 */
	private void initFieldFiller() {
		// populate fieldFiller so it can be used later when need occures
		char[] fillerArray = new char[Defaults.DataFormatter.FIELD_BUFFER_LENGTH];
		Arrays.fill(fillerArray, DEFAULT_FILLER_CHAR);

		try {
			fieldFiller = encoder.encode(CharBuffer.wrap(fillerArray));
		} catch (Exception ex) {
			throw new RuntimeException("Failed initialization of FIELD_FILLER buffer :" + ex);
		}
	}

	
}
