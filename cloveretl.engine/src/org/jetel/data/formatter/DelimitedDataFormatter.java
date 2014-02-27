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
package org.jetel.data.formatter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.UnsupportedCharsetException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.Defaults.DataFormatter;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Outputs delimited data record. Handles encoding of characters. Uses NIO classes
 *
 * @author     D.Pavlis
 * @since    July 25, 2002
 * @see        DataFormatter
 */
public class DelimitedDataFormatter extends AbstractFormatter {
	
    private String charSet = null;
	// Attributes
	private WritableByteChannel writer;
	private CharsetEncoder encoder;
	private String delimiters[];
	private int delimiterLength[];
	private CharBuffer charBuffer;
	private ByteBuffer dataBuffer;
	private int numFields;
	private String sFooter; 
	private String sHeader; 
	private ByteBuffer footer; 
	private ByteBuffer header; 
    
	// Associations

	// Operations
	
	public DelimitedDataFormatter() {
		writer = null;
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charSet = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;
	}
	
	public DelimitedDataFormatter(String charEncoder) {
		writer = null;
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charSet = charEncoder;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	@Override
	public void init(DataRecordMetadata metadata) {
		encoder = Charset.forName(charSet).newEncoder();
		encoder.reset();

		// create array of delimiters & initialize them
		delimiters = new String[metadata.getNumFields()];
		delimiterLength= new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			delimiters[i] = metadata.getField(i).getDelimiters()[0];
			delimiterLength[i] = delimiters[i].length();
		}
		
		numFields=metadata.getNumFields(); // buffer numFields
	}
	
	@Override
	public void reset() {
		if (writer != null && writer.isOpen()) {
			try{
				flush();
				writer.close();
			}catch(IOException ex){
				ex.printStackTrace();
			}
		}
		encoder.reset();
	}

    /* (non-Javadoc)
     * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
     */
    @Override
	public void setDataTarget(Object out) {
        // close previous output
        try {
			close();
		} catch (IOException e) {
			throw new JetelRuntimeException(e);
		}
        
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
	 * @throws IOException 
	 *
	 * @since    March 28, 2002
	 */
	@Override
	public void close() throws IOException {
		if (writer == null || !writer.isOpen()) {
			return;
		}

		try{
			flush();
		} finally {
			writer.close();
			writer = null;
		}
	}

	@Override
	public void finish() throws IOException {
		flush();
		writeFooter();
		flush();
	}

	/**
	 *  Description of the Method
	 *
	 * @since    March 28, 2002
	 */
	@Override
	public void flush() throws IOException{
		dataBuffer.flip();
		writer.write(dataBuffer);
		dataBuffer.clear();
	}

	/**
	 * Write record to output (or buffer).
	 * @param record
	 * @return Number of written bytes.
	 * @throws IOException
	 */
	@Override
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
        
		charBuffer.flip();
		return encode();
	}

	private int encode() throws IOException {
        CoderResult result;
		int newStart = dataBuffer.position();
        
        result = encoder.encode(charBuffer, dataBuffer, true);
        if (result.isError()){
            throw new IOException(result.toString() + " when converting to " + encoder.charset() + ": '" + charBuffer.toString() + "'");
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

	@Override
	public int writeFooter() throws IOException {
		if (footer == null && sFooter != null) {
	    	try {
				footer = ByteBuffer.wrap(sFooter.getBytes(encoder.charset().name()));
			} catch (UnsupportedEncodingException e) {
				throw new UnsupportedCharsetException(encoder.charset().name());
			}
		}
		if (footer != null) {
			dataBuffer.put(footer);
			footer.rewind();
			return footer.remaining();
		} else
			return 0;
	}

	@Override
	public int writeHeader() throws IOException {
		if (header == null && sHeader != null) {
	    	try {
				header = ByteBuffer.wrap(sHeader.getBytes(encoder.charset().name()));
			} catch (UnsupportedEncodingException e) {
				throw new UnsupportedCharsetException(encoder.charset().name());
			}
		}
		if (header != null) {
			dataBuffer.put(header);
			header.rewind();
			return header.remaining();
		} else 
			return 0;
	}

    public void setFooter(String footer) {
    	sFooter = footer;
    }

    public void setHeader(String header) {
    	sHeader = header;
    }
	
}
/*
 *  end class DelimitedDataFormatter
 */

