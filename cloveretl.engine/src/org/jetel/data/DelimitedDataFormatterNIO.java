/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002  David Pavlis
*
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

// FILE: c:/projects/jetel/org/jetel/data/DelimitedDataFormatter.java

package org.jetel.data;
import java.io.*;
import org.jetel.metadata.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;

/**
 * Outputs delimited data record. Handles encoding of characters. Uses NIO classes
 *
 * @author     D.Pavlis
 * @since    July 25, 2002
 * @see        DataFormatter
 */
public class DelimitedDataFormatterNIO implements DataFormatter {

	// Attributes
	private DataRecordMetadata metadata;
	private WritableByteChannel writer;
	private CharsetEncoder encoder;
	private String delimiters[];
	private int delimiterLength[];
	private CharBuffer charBuffer;
	private ByteBuffer dataBuffer;

	// Associations

	// Operations
	
	public DelimitedDataFormatterNIO(){
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		encoder = Charset.forName(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER).newEncoder();
		encoder.reset();
	}
	
	public DelimitedDataFormatterNIO(String charEncoder){
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		encoder = Charset.forName(charEncoder).newEncoder();
		encoder.reset();
	}
	
	/**
	 *  Description of the Method
	 *
	 * @param  out        Description of Parameter
	 * @param  _metadata  Description of Parameter
	 * @since             March 28, 2002
	 */
	public void open(OutputStream out, DataRecordMetadata _metadata) {
		this.metadata = _metadata;

		// create buffered input stream reader 
		writer = ((FileOutputStream) out).getChannel();

		// create array of delimiters & initialize them
		delimiters = new String[metadata.getNumFields()];
		delimiterLength= new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			delimiters[i] = metadata.getField(i).getDelimiter();
			delimiterLength[i] = delimiters[i].length();
		}
		charBuffer.clear(); // preventively clear the buffer
	}


	/**
	 *  Description of the Method
	 *
	 * @since    March 28, 2002
	 */
	public void close() {
		try{
			flushBuffer();
			writer.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @since    March 28, 2002
	 */
	public void flush() {
		
	}


	/**
	 *  Description of the Method
	 *
	 * @param  record           Description of Parameter
	 * @exception  IOException  Description of Exception
	 * @since                   March 28, 2002
	 */
	public void write(DataRecord record) throws IOException {
		String fieldVal;
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldVal=record.getField(i).toString();
			if ((fieldVal.length()+delimiterLength[i]) > charBuffer.remaining())
			{
				flushBuffer();
			}
			charBuffer.put(fieldVal);
			charBuffer.put(delimiters[i]);
		}
	}
	
	
	private void flushBuffer() throws IOException {
		CoderResult result;
		charBuffer.flip();
		dataBuffer.clear();
		result=encoder.encode(charBuffer,dataBuffer,false);
		dataBuffer.flip();
		writer.write(dataBuffer);
		charBuffer.clear();
	}

}
/*
 *  end class DelimitedDataFormatter
 */

