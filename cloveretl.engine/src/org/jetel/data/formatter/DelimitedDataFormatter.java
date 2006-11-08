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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.Defaults.DataFormatter;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Outputs delimited data record. Handles encoding of characters. Uses NIO classes
 *
 * @author     D.Pavlis
 * @since    July 25, 2002
 * @see        DataFormatter
 */
public class DelimitedDataFormatter implements Formatter {
	
    private final static String DELIMITER_SYSTEM_PROPERTY_NAME="line.separator";
    private String charSet = null;
	// Attributes
	private DataRecordMetadata metadata;
	private WritableByteChannel writer;
	private CharsetEncoder encoder;
	private String delimiters[];
	private int delimiterLength[];
	private CharBuffer charBuffer;
	private ByteBuffer dataBuffer;
	private int numFields;
	private boolean isRecordDelimiter;
    private String recordDelimiter;
    
	private static String NEW_LINE_STR;

	// Associations

	// Operations
	
	public DelimitedDataFormatter() {
		writer = null;
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		encoder = Charset.forName(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER).newEncoder();
		encoder.reset();
		NEW_LINE_STR=System.getProperty(DELIMITER_SYSTEM_PROPERTY_NAME,"\n");
		metadata = null;
	}
	
	public DelimitedDataFormatter(String charEncoder) {
		writer = null;
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charSet = charEncoder;
		encoder = Charset.forName(charEncoder).newEncoder();
		encoder.reset();
		NEW_LINE_STR=System.getProperty(DELIMITER_SYSTEM_PROPERTY_NAME,"\n");
		metadata = null;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata metadata) {
		this.metadata = metadata;

		// create array of delimiters & initialize them
		delimiters = new String[metadata.getNumFields()];
		delimiterLength= new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			delimiters[i] = metadata.getField(i).getDelimiter();
			delimiterLength[i] = delimiters[i].length();
		}
		
		numFields=metadata.getNumFields(); // buffer numFields
        
        //record delimiters initialization
        isRecordDelimiter = metadata.isSpecifiedRecordDelimiter();
        if(isRecordDelimiter) {
            recordDelimiter = metadata.getRecordDelimiter();
        }
	}

    /* (non-Javadoc)
     * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
     */
    public void setDataTarget(Object out) {
        // close previous output
        close();
        
        // create buffered input stream reader
        if (out == null) {
            writer = null;
        } else if (out instanceof WritableByteChannel) {
            writer = (WritableByteChannel) out;
        } else {
            writer = Channels.newChannel((OutputStream) out);
        }

        // preventively clear the buffer
        charBuffer.clear(); 
    }
    
	/**
	 *  Description of the Method
	 *
	 * @since    March 28, 2002
	 */
	public void close() {
		if (writer == null) {
			return;
		}

		try{
			dataBuffer.flip();
			writer.write(dataBuffer);
			dataBuffer.clear();
			writer.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
		writer = null;
	}


	/**
	 *  Description of the Method
	 *
	 * @since    March 28, 2002
	 */
	public void flush() {
		
	}

	/**
	 * Write record to output (or buffer).
	 * @param record
	 * @return Number of written bytes.
	 * @throws IOException
	 */
	public int write(DataRecord record) throws IOException {
		String fieldVal;
		charBuffer.clear();
		for (int i = 0; i < numFields; i++) {
			fieldVal=record.getField(i).toString();
			if ((fieldVal.length()+delimiterLength[i]) > charBuffer.remaining()) {
				throw new IOException("Insufficient buffer size");
			}
			
			charBuffer.put(fieldVal);
			charBuffer.put(delimiters[i]);
		}
        if(isRecordDelimiter) {
            charBuffer.put(recordDelimiter);
        }
        
		charBuffer.flip();
		return encode();
	}

	private int encode() throws IOException {
        CoderResult result;
		int newStart = dataBuffer.position();
        
        result = encoder.encode(charBuffer, dataBuffer, true);
        if (result.isError()){
            throw new IOException(result.toString() + " when converting to " + encoder.charset());
        }
        
        int encLen = dataBuffer.position() - newStart;
        if (!charBuffer.hasRemaining()) {	// all data encoded
        	return encLen;
        }
       	
        // write previous data
        dataBuffer.flip();
    	writer.write(dataBuffer);
    	dataBuffer.clear();
    	
        // encode remaining part of data
        result = encoder.encode(charBuffer, dataBuffer, true);
        if (result.isError()){
            throw new IOException(result.toString() + " when converting to " + encoder.charset());
        }

        return encLen + dataBuffer.position();
	}

/*	
	private void flushBuffer(boolean endOfData) throws IOException {
		CoderResult result;
		charBuffer.flip();

		dataBuffer.clear();

		do {  // Make sure we encode and output all the characters as bytes.
		    
            result=encoder.encode(charBuffer,dataBuffer,endOfData);
            if (result.isError()){
                throw new IOException(result.toString()+" when converting to "+encoder.charset());
            }
            dataBuffer.flip();
		    writer.write(dataBuffer);
		    dataBuffer.clear();

		} while (result.isOverflow());
		

		// Compact, don't clear in case the encoding needs the 
		// remaining characters to continue encoding.
		charBuffer.compact();

		if (endOfData) {
		    do {
			result = encoder.flush(dataBuffer);
            if (result.isError()){
                throw new IOException(result.toString()+" when converting to "+encoder.charset());
            }
			dataBuffer.flip();
			writer.write(dataBuffer);
			dataBuffer.clear();
		    } while (result.isOverflow());

		}
	}
*/

	/**
	 * Returns name of charset which is used by this formatter
	 * @return Name of charset or null if none was specified
	 */
	public String getCharsetName() {
		return(this.charSet);
	}
	
}
/*
 *  end class DelimitedDataFormatter
 */

