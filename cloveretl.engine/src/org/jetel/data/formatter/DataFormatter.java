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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Outputs common data record. Handles encoding of characters. Uses WriteableChannel.
 *
 * @author     Martin Zatopek, Javlin Consulting s.r.o. (www.javlinconsulting.cz)
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
    private byte[] recordDelimiter;
	private int[] delimiterLength;
	private int[] fieldLengths;
	private ByteBuffer dataBuffer;
	private String sFooter; 
	private String sHeader; 
	private ByteBuffer footer; 
	private ByteBuffer header; 

	private String[] excludedFieldNames;
	private int[] includedFieldIndices;

	// use space (' ') to fill/pad field
	private final static char DEFAULT_FILLER_CHAR = ' ';

	// Associations

	// Operations
	
	public DataFormatter(){
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
		charSet = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;
		metadata = null;
	}
	
	public DataFormatter(String charEncoder){
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
		charSet = charEncoder;
		metadata = null;
	}
	
	public void setExcludedFieldNames(String[] excludedFieldNames) {
		this.excludedFieldNames = excludedFieldNames;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata _metadata) {
		// create array of delimiters & initialize them
		// create array of field sizes & initialize them
		metadata = _metadata;
		encoder = Charset.forName(charSet).newEncoder();
		initFieldFiller();
		encoder.reset();
		delimiters = new byte[metadata.getNumFields()][];
        delimiterLength = new int[metadata.getNumFields()];
		fieldLengths = new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			if(metadata.getField(i).isDelimited()) {
                try {
                	String[] fDelimiters = metadata.getField(i).getDelimiters();
                	if (fDelimiters != null) { //for eof delimiter
    					delimiters[i] = fDelimiters[0].getBytes(charSet);
                	}
				} catch (UnsupportedEncodingException e) {
					// can't happen if we have encoder
				}
				delimiterLength[i] = delimiters[i] == null ? 0 : delimiters[i].length; //for eof delimiter
			} else {
				fieldLengths[i] = metadata.getField(i).getSize();
			}
		}
		try {
			if(metadata.isSpecifiedRecordDelimiter()) {
				recordDelimiter = metadata.getRecordDelimiters()[0].getBytes(charSet);
			}
		} catch (UnsupportedEncodingException e) {
			// can't happen if we have encoder
		}

		includedFieldIndices = metadata.fieldsIndicesComplement(excludedFieldNames);

		int lastFieldIndex = metadata.getNumFields() - 1;
		int lastIncludedFieldIndex = includedFieldIndices[includedFieldIndices.length - 1];

		if (lastIncludedFieldIndex < lastFieldIndex) {
			delimiters[lastIncludedFieldIndex] = delimiters[lastFieldIndex];
			delimiterLength[lastIncludedFieldIndex] = delimiterLength[lastFieldIndex];
			fieldLengths[lastIncludedFieldIndex] = fieldLengths[lastFieldIndex];
		}
	}

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
    public void setDataTarget(Object out) {
        close();
        
        // create buffered output stream reader 
        if (out == null) {
            writer = null;
        } else if (out instanceof WritableByteChannel) {
            writer = (WritableByteChannel) out;
        } else {
            writer = Channels.newChannel((OutputStream) out);
        }
    }
    
	/**
	 *  Description of the Method
	 *
	 * @since    March 28, 2002
	 */
	public void close() {
		if (writer == null || !writer.isOpen()) {
			return;
		}
		try{
			flush();
			writer.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
		writer = null;
	}

	public void finish() throws IOException {
		flush();
		writeFooter();
		flush();
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
	 * Write record to output (or buffer).
	 * @param record
	 * @return Number of written bytes.
	 * @throws IOException
	 */
	public int write(DataRecord record) throws IOException {
		int size;
		int encLen = 0;
		int i = 0;
		try {
			for (int index : includedFieldIndices) {
				i = index;
				if(metadata.getField(i).isDelimited()) {
					fieldBuffer.clear();
					record.getField(i).toByteBuffer(fieldBuffer, encoder);
					int fieldLen = fieldBuffer.position() + delimiterLength[i];
					if(fieldLen > dataBuffer.remaining()) {
						flush();
					}
					encLen += fieldLen;
					fieldBuffer.flip();
					dataBuffer.put(fieldBuffer);
					if (delimiters[i] != null) dataBuffer.put(delimiters[i]); //for eof delimiter
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
					encLen += size;
					fieldBuffer.flip();
					fieldBuffer.limit(fieldLengths[i]);
					dataBuffer.put(fieldBuffer);
					if (i == metadata.getNumFields() -1 && recordDelimiter != null){
						dataBuffer.put(recordDelimiter);
					}
				}
			}
		} catch (CharacterCodingException e) {
            throw new RuntimeException("Exception when converting the field value: " + record.getField(i).getValue()
            		+ " (field name: '" + record.getMetadata().getField(i).getName() + "') to " + encoder.charset()
            		+ ". (original cause: " + e.getMessage() + ") \n\nRecord: " +record.toString(), e);
		}
        return encLen;
	}
	
	/**
	 * Returns name of charset which is used by this formatter
	 * @return Name of charset or null if none was specified
	 */
	public String getCharsetName() {
		return(this.charSet);
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
