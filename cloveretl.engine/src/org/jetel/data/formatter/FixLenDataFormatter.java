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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.FixLenDataParser;
import org.jetel.exception.ComponentNotReadyException;
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

	private WritableByteChannel writer;
	private CharsetEncoder encoder;
	private int recordLength;
	private ByteBuffer fieldFillerBuf;
	private ByteBuffer recordFillerBuf;
	private String charSet = null;
    private boolean isRecordDelimiter;
    private byte[] recordDelimiter;
	private String sFooter; 
	private String sHeader; 
	private ByteBuffer footer; 
	private ByteBuffer header; 

    private int fieldCnt;
    private int[] fieldStart;
    private int[] fieldEnd;
    private int gapCnt;
    private int[] gapStart;
    private int[] gapEnd;

	// Attributes
	// use space (' ') to fill/pad field
	private final static char DEFAULT_FIELDFILLER_CHAR = ' ';
	private final static char DEFAULT_RECORDFILLER_CHAR = '=';
	
	/**
	 *  Constructor for the FixLenDataFormatter object
	 *
	 *@since    August 21, 2002
	 */
	public FixLenDataFormatter() {
		writer = null;
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charSet = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;
	}


	/**
	 *  Constructor for the FixLenDataFormatter object
	 *
	 *@param  charEncoder  Description of the Parameter
	 *@since               August 21, 2002
	 */
	public FixLenDataFormatter(String charEncoder) {
		writer = null;
		charSet = charEncoder;
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
	}


	/**
	 *  Initialization of the filler buffers
	 */
	private void initFieldFiller(char filler) {
		// populate fieldFiller so it can be used later when need occures
		char[] fillerArray = new char[Defaults.DataFormatter.FIELD_BUFFER_LENGTH];
		Arrays.fill(fillerArray, filler);
		
		try {
			fieldFillerBuf = encoder.encode(CharBuffer.wrap(fillerArray));
		} catch (Exception ex) {
			throw new RuntimeException("Failed initialization of filler buffers :" + ex);
		}
	}

	/**
	 *  Initialization of the filler buffers
	 */
	private void initRecordFiller(char filler) {
		// populate fieldFiller so it can be used later when need occures
		char[] fillerArray = new char[Defaults.DataFormatter.FIELD_BUFFER_LENGTH];
		Arrays.fill(fillerArray, filler);
		
		try {
			recordFillerBuf = encoder.encode(CharBuffer.wrap(fillerArray));
		} catch (Exception ex) {
			throw new RuntimeException("Failed initialization of filler buffers :" + ex);
		}
	}

	/**
	 * Specify which character should be used as filler for
	 * padding fields when outputting
	 * 
	 * @param filler	character used for padding
	 */
    Character fieldFiller;
	public void setFieldFiller(char filler) {
        this.fieldFiller = Character.valueOf(filler);
	}

    public Character getFieldFiller() {
        return fieldFiller;
    }

    Character recordFiller;
	public void setRecordFiller(char filler) {
        this.recordFiller = Character.valueOf(recordFiller);
	}

    public Character getRecordFiller() {
        return recordFiller;
    }

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
		// create array of field sizes & initialize them
        
		encoder = Charset.forName(charSet).newEncoder();
		initFieldFiller(fieldFiller == null ? DEFAULT_FIELDFILLER_CHAR : fieldFiller.charValue());
		initRecordFiller(recordFiller == null ? DEFAULT_RECORDFILLER_CHAR : recordFiller.charValue());
		encoder.reset();
        isRecordDelimiter = metadata.isSpecifiedRecordDelimiter();
        if(isRecordDelimiter) {
            try {
                recordDelimiter = metadata.getRecordDelimiters()[0].getBytes(charSet);
            } catch (UnsupportedEncodingException e) {
                throw new ComponentNotReadyException(e);
            }
        }
		recordLength = metadata.getRecordSize() > 0 ? metadata.getRecordSize() : metadata.getRecordSizeStripAutoFilling(); 

		if (dataBuffer.capacity() < recordLength + (isRecordDelimiter ? recordDelimiter.length : 0)) {
			throw new RuntimeException("Output buffer too small to hold data record " + metadata.getName());			
		}

		fieldCnt = metadata.getNumFields();
		fieldStart = new int[fieldCnt];
		fieldEnd = new int[fieldCnt];
		int prevEnd = 0;
		for (int fieldIdx = 0; fieldIdx < fieldCnt; fieldIdx++) {
			fieldStart[fieldIdx] = prevEnd + metadata.getField(fieldIdx).getShift();
			fieldEnd[fieldIdx] = fieldStart[fieldIdx] + metadata.getField(fieldIdx).getSize();
			prevEnd = fieldEnd[fieldIdx];
			if (fieldStart[fieldIdx] < 0 || fieldEnd[fieldIdx] > recordLength) {
				throw new ComponentNotReadyException("field boundaries cannot be outside record boundaries");
			}
		}
		// find gaps
		SortedMap<Integer, Integer> smap = new TreeMap<Integer, Integer>();
		for (int fieldIdx = 0; fieldIdx < fieldCnt; fieldIdx++) {
			smap.put(Integer.valueOf(fieldStart[fieldIdx]), Integer.valueOf(fieldIdx));
		}
		ArrayList<Integer> gapStartList = new ArrayList<Integer>();
		ArrayList<Integer> gapEndList = new ArrayList<Integer>();
		int gapPos = 0;
		for (Integer fieldIdx : smap.values()) {
			int fieldPos = fieldStart[fieldIdx.intValue()];
			if (fieldPos > gapPos) {
				gapStartList.add(new Integer(gapPos));
				gapEndList.add(new Integer(fieldPos));
			}
			if (fieldEnd[fieldIdx.intValue()] > gapPos) {
				gapPos = fieldEnd[fieldIdx.intValue()]; 
			}
		}
		if (recordLength > gapPos) {
			gapStartList.add(new Integer(gapPos));
			gapEndList.add(new Integer(recordLength));
		}
		gapCnt = gapStartList.size();
		gapStart = new int[gapCnt];
		gapEnd = new int[gapCnt];
		for (int gapIdx = 0; gapIdx < gapCnt; gapIdx++) {
			gapStart[gapIdx] = gapStartList.get(gapIdx).intValue();
			gapEnd[gapIdx] = gapEndList.get(gapIdx).intValue();
		}			
	}

	public void reset() {
		if (writer != null && writer.isOpen()) {
			try {
				flushBuffer();
				writer.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		encoder.reset();
		dataBuffer.clear();
	}
	
	public void finish() throws IOException {
		flush();
		writeFooter();
		flush();
	}
	
    /* (non-Javadoc)
     * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
     */
    public void setDataTarget(Object out) {
        close();

        if (out == null) {
            writer = null;
        } else if (out instanceof WritableByteChannel) {
            writer = (WritableByteChannel) out;
        } else {
            writer = Channels.newChannel((OutputStream) out);
        }

        // reset CharsetDecoder
        encoder.reset();
    }

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public int write(DataRecord record) throws IOException {
		int recPos = dataBuffer.position();
		int totalLen = recordLength + (isRecordDelimiter ? recordDelimiter.length : 0);

		if (recPos + totalLen > dataBuffer.capacity()) {
			flushBuffer();
			recPos = 0;
		}
		// write fields
		dataBuffer.limit(dataBuffer.capacity());
		int i = 0;
		try {
			for (i = 0; i < fieldCnt; i++) {
				dataBuffer.position(recPos + fieldStart[i]);
				record.getField(i).toByteBuffer(dataBuffer, encoder);
				int remn = recPos + fieldEnd[i] - dataBuffer.position();	// remaining bytes to be written
				if (remn > 0) {
					fieldFillerBuf.rewind();
					fieldFillerBuf.limit(remn);
					dataBuffer.put(fieldFillerBuf);				
				}
			}
		} catch (CharacterCodingException e) {
            throw new RuntimeException("Exception when converting the field value: " + record.getField(i).getValue() + " (field name: '" + record.getMetadata().getField(i).getName() + "') to " + encoder.charset() + ". (original cause: " + e.getMessage() + ") \n\nRecord: " +record.toString(), e);
		}
		// fill gaps
		for (i = 0; i < gapCnt; i++) {
			dataBuffer.position(0);	// to avoid exceptions being thrown while setting buffer limit
			dataBuffer.limit(recPos + gapEnd[i]);
			dataBuffer.position(recPos + gapStart[i]);
			recordFillerBuf.rewind();
			recordFillerBuf.limit(dataBuffer.remaining());
			dataBuffer.put(recordFillerBuf);
		}
        // write record delimiter
		if(isRecordDelimiter){
			dataBuffer.limit(recPos + recordLength + recordDelimiter.length);
			dataBuffer.position(recPos + recordLength);
			dataBuffer.put(recordDelimiter);
		}
        dataBuffer.limit(dataBuffer.capacity());
        dataBuffer.position(recPos + totalLen);
		return totalLen;
	}

	/**
	 *  Description of the Method
	 */
	public void close() {
		if (writer == null || !writer.isOpen()) {
			return;
		}
		try {
			flushBuffer();
			writer.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		writer = null;
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

	/* unused method
	private void flushBuffer(int limit) throws IOException {
		int savedLimit = dataBuffer.limit();
		dataBuffer.limit(limit);
		dataBuffer.flip();
		writer.write(dataBuffer);
		dataBuffer.position(limit);
		dataBuffer.limit(savedLimit);
		dataBuffer.compact();
		dataBuffer.position(dataBuffer.limit());
		dataBuffer.limit(dataBuffer.capacity());
	}
	*/

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
	 * 
	 * @return
	 */
	public String getCharSetName() {
		return(this.charSet);
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
/*
 *  end class FixLenDataFormatter
 */

