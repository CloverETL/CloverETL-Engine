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
package org.jetel.data.formatter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.Arrays;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.FixLenDataParser;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Outputs fix-len data record. Handles encoding of character based fields.
 *  Uses NIO classes
 *
 * @author     David Pavlis
 * @since      December 30, 2002
 * @see        FixLenDataParser
 * @revision   $Revision$
 */
public class FixLenDataFormatter implements Formatter {

	private ByteBuffer dataBuffer;
	private ByteBuffer fieldBuffer;
	private DataRecordMetadata metadata;

	private WritableByteChannel writer;
	private CharsetEncoder encoder;
	private int recordCounter;
	private int fieldLengths[];
	private int bufferSize;
	private ByteBuffer fieldFiller;
	private byte[] crLF;
	private String charSet = null;
	
	private boolean oneRecordPerLinePolicy = false;

	// Attributes
	// use space (' ') to fill/pad field
	private final static char DEFAULT_FILLER_CHAR = ' ';
	
	/**
	 *  Constructor for the FixLenDataFormatter object
	 *
	 *@since    August 21, 2002
	 */
	public FixLenDataFormatter() {
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
		encoder = Charset.forName(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER ).newEncoder();
		initFieldFiller();
		encoder.reset();
		crLF=System.getProperty("line.separator","\n").getBytes();
	}


	/**
	 *  Constructor for the FixLenDataFormatter object
	 *
	 *@param  charEncoder  Description of the Parameter
	 *@since               August 21, 2002
	 */
	public FixLenDataFormatter(String charEncoder) {
		charSet = charEncoder;
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
		encoder = Charset.forName(charEncoder).newEncoder();
		initFieldFiller();
		encoder.reset();
		crLF=System.getProperty("line.separator","\n").getBytes();
	}


	/**
	 *  Initialization of the FieldFiller buffer
	 */
	private void initFieldFiller() {
		// populate fieldFiller so it can be used later when need occures
		char[] fillerArray = new char[Defaults.DataFormatter.FIELD_BUFFER_LENGTH];
		Arrays.fill(fillerArray,DEFAULT_FILLER_CHAR);

		try {
			fieldFiller = encoder.encode(CharBuffer.wrap(fillerArray));
		} catch (Exception ex) {
			throw new RuntimeException("Failed initialization of FIELD_FILLER buffer :" + ex);
		}
	}


	/**
	 *  Description of the Method
	 *
	 *@param  out        Description of the Parameter
	 *@param  _metadata  Description of the Parameter
	 */
	public void open(OutputStream out, DataRecordMetadata _metadata) {
		CoderResult result;
		this.metadata = _metadata;

		writer = ((FileOutputStream) out).getChannel();

		// create array of field sizes & initialize them
		fieldLengths = new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldLengths[i] = metadata.getField(i).getSize();
		}
		encoder.reset();
		// reset CharsetDecoder
		recordCounter = 0;
		// reset record counter
	}


	/**
	 *  Description of the Method
	 *
	 *@param  record           Description of the Parameter
	 *@exception  IOException  Description of the Exception
	 */
	public void write(DataRecord record) throws IOException {
		int size;
		for (int i = 0; i < metadata.getNumFields(); i++) {
			
			if (fieldLengths[i] > dataBuffer.remaining()) {
				flushBuffer();
			}
			fieldBuffer.clear();
			record.getField(i).toByteBuffer(fieldBuffer, encoder);
			size = fieldBuffer.position();
			if (size < fieldLengths[i]) {
				fieldFiller.rewind();
				fieldFiller.limit(fieldLengths[i]-size);
				fieldBuffer.put(fieldFiller);
				
			}
			fieldBuffer.flip();
			fieldBuffer.limit(fieldLengths[i]);
			dataBuffer.put(fieldBuffer);
		}
		if(oneRecordPerLinePolicy){
			if (dataBuffer.remaining()<crLF.length){
				flushBuffer();
			}
			dataBuffer.put(crLF);
		}
	}



	/**
	 *  Description of the Method
	 */
	public void close() {
		try {
			flushBuffer();
			writer.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}


	/**
	 *  Description of the Method
	 *
	 *@exception  IOException  Description of the Exception
	 */
	private void flushBuffer() throws IOException {
		dataBuffer.flip();
		writer.write(dataBuffer);
		dataBuffer.clear();
	}


	/**
	 *  Flushes the content of internal data buffer
	 */
	public void flush() {
		try {
			flushBuffer();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}


	/**
	 *  Sets OneRecordPerLinePolicy.
	 * @see org.jetel.data.formatter.Formatter#setOneRecordPerLinePolicy(boolean)
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
		oneRecordPerLinePolicy = b;
	}

	/**
	 * Sets line separator char(s) - allows to specify
	 * different than default EOL character (\n).
	 * 
	 * @param separator
	 */
	public void setLineSeparator(String separator){
		crLF=separator.getBytes();
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean getOneRecordPerLinePolicy() {
		return(this.oneRecordPerLinePolicy);
	}
	
	/**
	 * 
	 * @return
	 */
	public String getCharSetName() {
		return(this.charSet);
	}
	
	
}
/*
 *  end class FixLenDataFormatter
 */

