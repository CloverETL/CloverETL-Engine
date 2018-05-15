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
import java.nio.BufferOverflowException;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.QuotingDecoder;

/**
 * Outputs common data record. Handles encoding of characters. Uses WriteableChannel.
 *
 * @author     Martin Zatopek, Javlin Consulting s.r.o. (www.javlinconsulting.cz)
 * @since      26. 9. 2005
 * @see        Formatter
 */
public class DataFormatter extends AbstractFormatter {
	private String charSet = null;
	private CloverBuffer fieldBuffer;
	private CloverBuffer fieldFiller;
	private int numberOfBytesPerFillerChar; //this is size of DEFAULT_FILLER_CHAR character in bytes in given charset
	private DataRecordMetadata metadata;
	private WritableByteChannel writer;
	private CharsetEncoder encoder;
    private byte[][] delimiters;
    private byte[] recordDelimiter;
	private int[] delimiterLength;
	private int[] fieldLengths;
	private boolean[] quotedFields;
	private boolean[] byteBasedFields;
	private CloverBuffer dataBuffer;
	private String sFooter; 
	private String sHeader; 
	private CloverBuffer footer; 
	private CloverBuffer header; 
	private boolean quotedStrings;
	/** This flag indicates the last record does not have record delimiter **/
	private boolean skipLastRecordDelimiter = false;
	/** This flag is just indication of first record to be written.
	 * This is just implementation detail of skipping last record delimiter. **/
	private boolean firstRecord;
	
	private String[] excludedFieldNames;
	private int[] includedFieldIndices;

	private QuotingDecoder quotingDecoder = new QuotingDecoder();
	
	static Log logger = LogFactory.getLog(DataFormatter.class);
	
	// use space (' ') to fill/pad field
	private final static char DEFAULT_FILLER_CHAR = ' ';

	// Associations

	// Operations
	
	public DataFormatter(){
		dataBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORDS_BUFFER_SIZE);
		fieldBuffer = CloverBuffer.allocateDirect(Defaults.Record.FIELD_INITIAL_SIZE, Defaults.Record.FIELD_LIMIT_SIZE);
		charSet = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;
		metadata = null;
	}
	
	public DataFormatter(String charEncoder){
		dataBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORDS_BUFFER_SIZE);
		fieldBuffer = CloverBuffer.allocateDirect(Defaults.Record.FIELD_INITIAL_SIZE, Defaults.Record.FIELD_LIMIT_SIZE);
		charSet = charEncoder;
		metadata = null;
	}
	
	public DataFormatter(DataFormatter parent) {
		// can't be shared without flushing every record, too slow 
		dataBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORDS_BUFFER_SIZE);
		fieldBuffer = parent.fieldBuffer; // shared buffer, potentially dangerous
		charSet = parent.charSet;
		metadata = null;
	}
	
	public void setExcludedFieldNames(String[] excludedFieldNames) {
		this.excludedFieldNames = excludedFieldNames;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	@Override
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
		quotedFields = new boolean[metadata.getNumFields()];
		byteBasedFields = new boolean[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			if(metadata.getField(i).isDelimited()) {
				quotedFields[i] = quotedStrings 
						&& metadata.getField(i).getDataType() != DataFieldType.BYTE
						&& metadata.getField(i).getDataType() != DataFieldType.CBYTE;
                try {
                	String[] fDelimiters = null;
                	fDelimiters = metadata.getField(i).getDelimitersWithoutRecordDelimiter(false); //record delimiter is written manually
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
			byteBasedFields[i] = metadata.getField(i).isByteBased();
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
			// CLO-5083: removed possibly incorrect code
//			fieldLengths[lastIncludedFieldIndex] = fieldLengths[lastFieldIndex];
		}
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
	public void setDataTarget(Object out) throws IOException {
        close();
        
        // create buffered output stream reader 
        if (out == null) {
            writer = null;
        } else if (out instanceof WritableByteChannel) {
            writer = (WritableByteChannel) out;
        } else {
            writer = Channels.newChannel((OutputStream) out);
        }
        
        firstRecord = true;
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
		try {
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
	 * @throws IOException
	 *
	 * @since    March 28, 2002
	 */
	@Override
	public void flush() throws IOException {
		dataBuffer.flip();
		int remaining = dataBuffer.buf().remaining();
		int written = writer.write(dataBuffer.buf());
		if (written != remaining) {
			byte[] dst = new byte[remaining - written];
			dataBuffer.buf().get(dst, written, remaining - written);
			dataBuffer = CloverBuffer.wrap(dst);
			logger.warn("Attempt to write data from buffer is incomplete. Attempted to write " + remaining + "b. Written " + written + "b.");
		} else {
			dataBuffer.clear();
		}
	}

	/**
	 * Write record to output (or buffer).
	 * @param record
	 * @return Number of written bytes.
	 * @throws IOException
	 */
	@Override
	public int write(DataRecord record) throws IOException {
		int size;
		int encLen = 0;
		int i = 0;
		try {
			//write record delimiter if necessary
			if (skipLastRecordDelimiter) {
				//in case the last record delimiter should be skipped
				//record delimiter is actually written at the beginning of each written record expect the first one
				if (!firstRecord) {
					encLen += writeRecordDelimiter();
				} else {
					firstRecord = false;
				}
			}
			for (int index : includedFieldIndices) {
				i = index;
				if(metadata.getField(i).isDelimited()) {
					fieldBuffer.clear();
					if (quotedFields[i]) {
						//could it be written in better way? faster?
						fieldBuffer.put(encoder.encode(CharBuffer.wrap(quotingDecoder.encode(record.getField(i).toString()))));
					} else {
						record.getField(i).toByteBuffer(fieldBuffer, encoder);
					}
					int fieldLen = fieldBuffer.position() + delimiterLength[i];
					if(fieldLen > dataBuffer.remaining()) {
						flush();
					}
					encLen += fieldLen;
					fieldBuffer.flip();
					dataBuffer.put(fieldBuffer);
					if (delimiters[i] != null) dataBuffer.put(delimiters[i]); //for eof delimiter
				} else { //fixlen field
					fieldBuffer.clear();
					size = record.getField(i).toByteBuffer(fieldBuffer, encoder, fieldLengths[i]);
					if (byteBasedFields[i]) {
						//the size returned from toByteBuffer() method is always 0 for byte based fields 
						//let's count it in other way
						size = fieldBuffer.position();
						if (size > fieldLengths[i]) {
							//only byte based fields need to be manually truncated
							//the string based fields are already truncated in toByteBuffer function
							fieldBuffer.flip();
							fieldBuffer.limit(fieldLengths[i]);
						} else {
							fieldBuffer.flip();
						}
					} else {
						if (size < fieldLengths[i]) { //byte fields are not auto-filled
							fieldFiller.rewind();
							fieldFiller.limit((fieldLengths[i] - size) * numberOfBytesPerFillerChar);
							fieldBuffer.put(fieldFiller);
							fieldBuffer.flip();
						} else {
							fieldBuffer.flip();
						}
					}
					
					if (dataBuffer.remaining() < fieldBuffer.limit()) {
						flush();
					}
					dataBuffer.put(fieldBuffer);
					encLen += fieldBuffer.limit();
				}
			}
			//write record delimiter if necessary
			if (!skipLastRecordDelimiter) {
				encLen += writeRecordDelimiter();
			}
		} catch (BufferOverflowException exception) {
    		throw new RuntimeException("The size of data buffer is only " + dataBuffer.maximumCapacity()
    				+ ". Set appropriate parameter in defaultProperties file.", exception);
		} catch (CharacterCodingException e) {
            throw new RuntimeException("Exception when converting the field value: " + record.getField(i).getValue()
            		+ " (field name: '" + record.getMetadata().getField(i).getName() + "') to " + encoder.charset()
            		+ ".\nRecord: " +record.toString(), e);
		}
        return encLen;
	}
	
	/**
	 * Writes record delimiter.
	 * @return length of written record delimiter
	 */
	private int writeRecordDelimiter() {
		if (recordDelimiter != null){
			dataBuffer.put(recordDelimiter);
			return recordDelimiter.length;
		} else {
			return 0;
		}
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
		// CL-2607 - minor improvement
		// allocate fieldFiller to the maximum size of fixed-length fields
		int maxFieldSize = Integer.MIN_VALUE;
		for (int i = 0; i < metadata.getNumFields(); i++) {
			DataFieldMetadata field = metadata.getField(i);
			if (field.isFixed() && field.getSize() > maxFieldSize) {
				maxFieldSize = field.getSize();
			}
		}
		
		if (maxFieldSize >= 0) {
			// populate fieldFiller so it can be used later when need comes
			char[] fillerArray = new char[maxFieldSize];
			Arrays.fill(fillerArray, DEFAULT_FILLER_CHAR);

			try {
				fieldFiller = CloverBuffer.wrap(encoder.encode(CharBuffer.wrap(fillerArray)));
				numberOfBytesPerFillerChar = fieldFiller.limit() / fillerArray.length;
			} catch (Exception ex) {
				throw new RuntimeException("Failed initialization of FIELD_FILLER buffer :" + ex);
			}
		}
	}

	@Override
	public int writeFooter() throws IOException {
		if (footer == null && sFooter != null) {
	    	try {
				footer = CloverBuffer.wrap(sFooter.getBytes(encoder.charset().name()));
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
		if (append && appendTargetNotEmpty) {
			return 0;
		}
		if (header == null && sHeader != null) {
	    	try {
				header = CloverBuffer.wrap(sHeader.getBytes(encoder.charset().name()));
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

    public void setQuotedStrings(boolean quotedStrings) {
    	this.quotedStrings = quotedStrings;
    }
    
    public boolean getQuotedStrings() {
    	return quotedStrings;
    }
    
    public void setQuoteChar(Character quoteChar) {
    	quotingDecoder.setQuoteChar(quoteChar);
    }
    
    public void setSkipLastRecordDelimiter(boolean skipLastRecordDelimiter) {
    	this.skipLastRecordDelimiter = skipLastRecordDelimiter;
    }
    
}
