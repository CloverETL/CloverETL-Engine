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
import java.io.UnsupportedEncodingException;
import java.nio.CharBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Outputs data record in form coherent with given mask. 
 * Handles encoding of characters. Uses WriteableChannel.
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 27, 2006
 *
 */
public class TextTableFormatter extends AbstractFormatter {
	
	private DataRecordMetadata metadata;
	private WritableByteChannel writerChannel;
	private CloverBuffer fieldBuffer; 
	private CloverBuffer dataBuffer;
	private CharsetEncoder encoder;
	private String charSet = null;
	
	private List<DataRecord> dataRecords;
	private CharBuffer blank;
	private CharBuffer horizontal;
	private boolean setOutputFieldNames = true;
//	private int rowSize = 0;
	private int leftBytes = 0;
	private DataFieldParams[] maskAnalize;
	private String[] mask;
	private boolean writeHeader = false;
	private boolean showCounter;
	private int counter;
	private byte[] header;
	private byte[] prefix;
	private String sCounter;
	private int counterLenght;
	private int prefixOffset; 
	private int headerOffset;
	
	private byte[] trashIDHeader;
	private byte[] trashID;
	private int trashIDLenght;
	private int trashIDHeaderOffset;
	private int trashIDOffset;
	private boolean showTrashID;
	
	/** this switch indicates, that header has been written already for current set of records; 
	 * it has to be reset to false just after footer is written */
	private boolean headerWritten = false;

	public static final int MAX_ROW_ANALYZED = 20;
	private int PADDING_SPACE = 3;

	private static final byte[] TABLE_CORNER = new byte[] {('+')};
	private static final char TABLE_HORIZONTAL = '-';
	private static final char TABLE_BLANK = ' ';
	private static final byte[] TABLE_VERTICAL = new byte[] {('|')};
	private static final byte[] NL = new byte[] {('\n')};
	
	/**
	 * Constructor without parameters
	 */
	public TextTableFormatter(){
		charSet = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;
	}
	
	/**
	 * Constructor
	 * 
	 * @param charEncoder charset for coding characters
	 */
	public TextTableFormatter(String charEncoder){
		charSet = charEncoder;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	@Override
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		this.metadata = _metadata;
		encoder = Charset.forName(charSet).newEncoder();
		encoder.reset();

		// create buffered output stream writer and buffers 
		dataBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORDS_BUFFER_SIZE);
		fieldBuffer = CloverBuffer.allocateDirect(Defaults.Record.FIELD_INITIAL_SIZE, Defaults.Record.FIELD_LIMIT_SIZE);
		//if mask is not given create default mask
		if (mask == null) {
			maskAnalize = new DataFieldParams[metadata.getNumFields()];
			for (int i=0;i<metadata.getNumFields();i++){
				maskAnalize[i] = new DataFieldParams(metadata.getField(i).getName(), i, 0);
			}
		} else {
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (int i=0;i<metadata.getNumFields();i++){
				map.put(metadata.getField(i).getName(), i);
			}
			maskAnalize = new DataFieldParams[mask.length];
			for (int i=0;i<mask.length;i++){
				if (map.get(mask[i]) == null)
					throw new ComponentNotReadyException("Exception: Field '" + mask[i] + "' not found.");
				maskAnalize[i] = new DataFieldParams(mask[i], map.get(mask[i]), 0);
			}
		}
		dataRecords = new LinkedList<DataRecord>();
	}
	
	@Override
	public void reset() {
		if (writerChannel != null && writerChannel.isOpen()) {
			try {
				flush();
				writerChannel.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		encoder.reset();
		dataBuffer.clear();
		fieldBuffer.clear();
		dataRecords.clear();
		headerWritten = false;
		writeHeader = false;
	}

	@Override
	public void finish() throws IOException {
		flush();
		writeFooter();
		flush();
	}
	
    /* (non-Javadoc)
     * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
     */
    @Override
	public void setDataTarget(Object out) {
        close();
        writerChannel = (WritableByteChannel) out;
    }
    
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	@Override
	public void close() {
        if (writerChannel == null || !writerChannel.isOpen()) {
            return;
        }
		try{
			flush();
			writerChannel.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}

	private int writeRecord(DataRecord record) throws IOException {
        int sentBytes=0;
        int mark;
        int lenght;
        
        sentBytes += writeString(TABLE_VERTICAL);

        if (showTrashID) {
			fieldBuffer.clear();
			fieldBuffer.put(trashID);
			fieldBuffer.flip();

			blank.clear();
			lenght = trashIDOffset - fieldBuffer.limit();
			blank.limit(lenght > 0 ? lenght : 0);

			if (dataBuffer.remaining() < fieldBuffer.limit()+blank.capacity()){
				directFlush();
			}
            mark=dataBuffer.position();

			//put field value to data buffer
			dataBuffer.put(fieldBuffer);
			dataBuffer.put(encoder.encode(blank));

            sentBytes += dataBuffer.position()-mark;
            sentBytes += writeString(TABLE_VERTICAL);
        }

        if (showCounter) {
            counter++;
            sCounter = Integer.toString(counter);
            
			//change field value to bytes
			fieldBuffer.clear();
			fieldBuffer.put(prefix);
			fieldBuffer.put(sCounter.getBytes(charSet));
			fieldBuffer.flip();
            
			blank.clear();
			lenght = prefixOffset - fieldBuffer.limit();
			blank.limit(lenght > 0 ? lenght : 0);

			if (dataBuffer.remaining() < fieldBuffer.limit()+blank.capacity()){
				directFlush();
			}
            mark=dataBuffer.position();
			
			//put field value to data buffer
			dataBuffer.put(fieldBuffer);
			dataBuffer.put(encoder.encode(blank));
            
            sentBytes+=dataBuffer.position()-mark;
            sentBytes += writeString(TABLE_VERTICAL);
        }
        
		//for each record field which is in mask change its name to value
		int i = 0;
		try {
	        DataField dataField;
			for (i=0;i<maskAnalize.length;i++){
				//change field value to bytes
				fieldBuffer.clear();
				dataField = record.getField(maskAnalize[i].index);
				dataField.toByteBuffer(fieldBuffer, encoder);
				fieldBuffer.flip();
	            
				blank.clear();
				// (new String(dataField.toString().getBytes(charSet)).length()); - right: encodes to charset, another parser decodes from charset. 
				//                                                                         So you takes the lenght from original string 
				// fieldBuffer.limit() is wrong too - encoding
				lenght = maskAnalize[i].length - dataField.toString().length();  
				blank.limit(lenght > 0 ? lenght : 0); // analyzed just n record -> some rows can be longer  

				if (dataBuffer.remaining() < fieldBuffer.limit()+blank.capacity()){
					directFlush();
				}
	            mark=dataBuffer.position();

				//put field value to data buffer
				dataBuffer.put(fieldBuffer);
				dataBuffer.put(encoder.encode(blank));
	            
	            sentBytes+=dataBuffer.position()-mark;
	            sentBytes += writeString(TABLE_VERTICAL);
			}
	        sentBytes += writeString(NL);
		} catch (CharacterCodingException e) {
            throw new RuntimeException("Exception when converting the field value: " + record.getField(i).getValue() + " (field name: '" + record.getMetadata().getField(i).getName() + "') to " + encoder.charset() + ". (original cause: " + e.getMessage() + ") \n\nRecord: " +record.toString(), e);
		}
        return sentBytes;
	}

	@Override
	public int writeHeader() throws IOException {
		if (!setOutputFieldNames || !writeHeader || headerWritten) return 0; // writeHeader depends on MAX_COUNT_ANALYZED_COUNT
		if (!isMaskAnalized()) {
			analyzeRows(dataRecords, setOutputFieldNames);
		}
        int sentBytes=0;
        sentBytes += writeString(TABLE_CORNER);
        if (showTrashID) {
        	sentBytes += writeString(horizontal, trashIDLenght);
            sentBytes += writeString(TABLE_CORNER);
        }
        if (showCounter) {
        	sentBytes += writeString(horizontal, counterLenght);
            sentBytes += writeString(TABLE_CORNER);
        }
        for (int i=0; i<maskAnalize.length; i++) {
        	sentBytes += writeString(horizontal, maskAnalize[i].length);
            sentBytes += writeString(TABLE_CORNER);
        }
        sentBytes += writeString(NL);
        
		DataFieldMetadata[] fMetadata = metadata.getFields();
		String fName;
        sentBytes += writeString(TABLE_VERTICAL);
        if (showTrashID) {
        	sentBytes += writeString(trashIDHeader);
        	sentBytes += writeString(blank, trashIDHeaderOffset-trashIDHeader.length);
            sentBytes += writeString(TABLE_VERTICAL);
        }
        if (showCounter) {
        	sentBytes += writeString(header);
        	sentBytes += writeString(blank, headerOffset-header.length);
            sentBytes += writeString(TABLE_VERTICAL);
        }
        for (int i=0; i<maskAnalize.length; i++) {
        	fName = fMetadata[maskAnalize[i].index].getLabelOrName();
        	sentBytes += writeString(fName.getBytes(charSet));
        	sentBytes += writeString(blank, maskAnalize[i].length-fName.length());
            sentBytes += writeString(TABLE_VERTICAL);
        }
        sentBytes += writeString(NL);
        
        sentBytes += writeString(TABLE_CORNER);
        if (showTrashID) {
        	sentBytes += writeString(horizontal, trashIDLenght);
            sentBytes += writeString(TABLE_CORNER);
        }
        if (showCounter) {
        	sentBytes += writeString(horizontal, counterLenght);
            sentBytes += writeString(TABLE_CORNER);
        }
        for (int i=0; i<maskAnalize.length; i++) {
        	sentBytes += writeString(horizontal, maskAnalize[i].length);
            sentBytes += writeString(TABLE_CORNER);
        }
        sentBytes += writeString(NL);

        return sentBytes;
	}
	
	@Override
	public int writeFooter() throws IOException {
		if (!setOutputFieldNames) return 0;
		if (!writeHeader) {
			writeHeader = true;
			writeHeader();
			writeHeader = false;
		}
		if (!isMaskAnalized()) {
			flush();
		}
        int sentBytes=0;
        sentBytes += writeString(TABLE_CORNER);
        if (showTrashID) {
        	sentBytes += writeString(horizontal, trashIDLenght);
            sentBytes += writeString(TABLE_CORNER);
        }
        if (showCounter) {
        	sentBytes += writeString(horizontal, counterLenght);
            sentBytes += writeString(TABLE_CORNER);
        }
        for (int i=0; i<maskAnalize.length; i++) {
        	sentBytes += writeString(horizontal, maskAnalize[i].length);
            sentBytes += writeString(TABLE_CORNER);
        }
        sentBytes += writeString(NL);

        ByteBufferUtils.flush(dataBuffer.buf(), writerChannel); //xxx
		return sentBytes;
	}
	
	private int writeString(byte[] buffer) throws IOException {
        //int sentBytes=0;
        //int mark;
		if (dataBuffer.remaining() < buffer.length){
			directFlush();
		}
        //mark=dataBuffer.position();
        dataBuffer.put(buffer);
        //sentBytes+=dataBuffer.position()-mark;
		return new String(buffer).getBytes(charSet).length; // encoding
	}
	
	private int writeString(CharBuffer buffer, int lenght) throws IOException {
		if (lenght <= 0) return 0;
        int sentBytes=0;
        int mark;
        buffer.clear();
        buffer.limit(lenght);
		if (dataBuffer.remaining() < buffer.limit()){
			directFlush();
		}
        mark=dataBuffer.position();
		dataBuffer.put(encoder.encode(buffer));
        sentBytes+=dataBuffer.position()-mark;
		
		return sentBytes;
	}
	
	/**
	 * Writes record as 'write' function, but likewise can better format the rows.
	 * For MAX_COUNT_ANALYZED_COUNT rows return 0. Then returns count of all written rows.
	 * 
	 * @param record
	 * @throws IOException 
	 */
	@Override
	public int write(DataRecord record) throws IOException {
		int size;
		if (dataRecords != null) {
			dataRecords.add(record.duplicate());
			writeHeader = true;
			if (dataRecords.size() < MAX_ROW_ANALYZED) {
				return 0;
			}
			analyzeRows(dataRecords, setOutputFieldNames);
			size = writeHeader();
			headerWritten = true;
			for (DataRecord dataRecord : dataRecords) {
				size += writeRecord(dataRecord);
			}
			dataRecords.clear();
			return size;
		} else //xxx
			throw new NullPointerException("dataRecords cannot be null"); //xxx
	}
	
	private void analyzeRows(List<DataRecord> dataRecords, boolean header) {
		int lenght = 0;
		int max = 0;
		Object o;
		for (DataRecord dataRecord : dataRecords) {
			for (int i=0; i<maskAnalize.length; i++) {
				try {
					o = dataRecord.getField(maskAnalize[i].index);
					if (o != null) {
						lenght = new String(o.toString().getBytes(charSet)).length(); // encoding
					}
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				maskAnalize[i].length = maskAnalize[i].length < lenght ? lenght : maskAnalize[i].length;
			}
		}
		if (header) {
			DataFieldMetadata[] fMetadata = metadata.getFields();
			for (int i=0; i<maskAnalize.length; i++) {
				lenght = fMetadata[maskAnalize[i].index].getLabelOrName().length();
				maskAnalize[i].length = maskAnalize[i].length < lenght ? lenght : maskAnalize[i].length;
			}
		}
		if (PADDING_SPACE != 0) {
			for (int i=0; i<maskAnalize.length; i++) {
				maskAnalize[i].length += PADDING_SPACE;
//				rowSize += maskAnalize[i].length;
			}
			PADDING_SPACE = 0;
		}
//		rowSize++;
		
		for (DataFieldParams dataFieldParams : maskAnalize) {
			max = max > dataFieldParams.length ? max : dataFieldParams.length;
		}
		max = max > counterLenght ? max : counterLenght;
		max = max > trashIDLenght ? max : trashIDLenght;
		StringBuilder sb = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		for (int i = 0; i < max; i++) {
			sb.append(TABLE_BLANK);
			sb2.append(TABLE_HORIZONTAL);
		}
		blank = CharBuffer.wrap(sb.toString());
		horizontal = CharBuffer.wrap(sb2.toString());
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	@Override
	public void flush() throws IOException {
		if (dataRecords != null && dataRecords.size()>0) {
			if (!isMaskAnalized()) //xxx
				analyzeRows(dataRecords, setOutputFieldNames);
			ByteBufferUtils.flush(dataBuffer.buf(), writerChannel);
			leftBytes = writeHeader();
			for (DataRecord dataRecord : dataRecords) {
				leftBytes += writeRecord(dataRecord);
			}
			dataRecords.clear();
		}
		ByteBufferUtils.flush(dataBuffer.buf(), writerChannel);
	}
	
	private void directFlush() throws IOException {
		ByteBufferUtils.flush(dataBuffer.buf(), writerChannel);
	}

	public int getLeftBytes() {
		return leftBytes;
	}
	
	public void setMask(String[] mask) {
		this.mask = mask;
	}
	
	/**
	 * Returns name of charset which is used by this formatter
	 * @return Name of charset or null if none was specified
	 */
	public String getCharsetName() {
		return(this.charSet);
	}

	public void setOutputFieldNames(boolean setOutputFieldNames) {
		this.setOutputFieldNames = setOutputFieldNames;
	}
	
	private boolean isMaskAnalized() {
		for (DataFieldParams params: maskAnalize) {
			if (params.length > 0) {
				return true;
			}
		}
		return false;
	}
	
	public void showCounter(String header, String prefix) {
		this.showCounter = true;
		try {
			this.header = header.getBytes(encoder.charset().name());
			this.prefix = prefix.getBytes(encoder.charset().name());
		} catch (UnsupportedEncodingException e) {
			throw new UnsupportedCharsetException(encoder.charset().name());
		}
		//int iMax = Integer.toString(Integer.MAX_VALUE).length();
		int iHeader = header.length();
		int iPrefix = prefix.length();
		counterLenght = iHeader > iPrefix + 5 ? iHeader : iPrefix + 5;
		prefixOffset = counterLenght + this.prefix.length - prefix.length();
		headerOffset = counterLenght + this.header.length - header.length();
	}
	
	/**
	 * Private class for storing data field name, its index and lenght in mask
	 */
	private static class DataFieldParams {
		
		int index;
		int length;
		
		DataFieldParams(String name,int index, int length){
			this.index = index;
			this.length = length;
		}
	}

	/**
	 * Sets trashID that for printing.
	 * @param trashIDHeader
	 * @param trashID
	 */
	public void showTrashID(String trashIDHeader, String trashID) {
		showTrashID = true;
		try {
			this.trashIDHeader = trashIDHeader.getBytes(encoder.charset().name());
			this.trashID = trashID.getBytes(encoder.charset().name());
		} catch (UnsupportedEncodingException e) {
			throw new UnsupportedCharsetException(encoder.charset().name());
		}
		int iTrashIDHeader = trashIDHeader.length();
		int iTrashID = trashID.length();
		trashIDLenght = iTrashID + 1 > iTrashIDHeader ? iTrashID + 1 : iTrashIDHeader;
		trashIDOffset = trashIDLenght + this.trashID.length - iTrashID;
		trashIDHeaderOffset = trashIDLenght + this.trashIDHeader.length - iTrashIDHeader;
	}
}

