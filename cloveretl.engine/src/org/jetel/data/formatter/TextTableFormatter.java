
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ByteBufferUtils;

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
public class TextTableFormatter implements Formatter {
	
	private DataRecordMetadata metadata;
	private WritableByteChannel writer;
	private int[] maskIndex;
	private int lastIndex;
	private String fieldName;
	private ByteBuffer fieldBuffer; 
	private ByteBuffer dataBuffer;
	private CharsetEncoder encoder;
	private String charSet = null;
	
	private List<DataRecord> dataRecords;
	private CharBuffer blank;
	private CharBuffer horizontal;
	private boolean header = true;
	private int rowSize = 0;
	private int leftBytes = 0;
	private DataFieldParams[] maskAnalize;
	private String[] mask;

	private static final int MAX_COUNT_ANALYZED_COUNT = 20;
	private static final int PADDING_SPACE = 3;

	private static final byte[] TABLE_CORNER = new byte[] {('+')};
	private static final byte[] TABLE_HORIZONTAL = new byte[] {('-')};
	private static final byte[] TABLE_VERTICAL = new byte[] {('|')};
	private static final byte[] NL = new byte[] {('\n')};
	
	/**
	 * Constructor without parameters
	 */
	public TextTableFormatter(){
		encoder = Charset.forName(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER).newEncoder();
		encoder.reset();
	}
	
	/**
	 * Constructor
	 * 
	 * @param charEncoder charset for coding characters
	 */
	public TextTableFormatter(String charEncoder){
		charSet = charEncoder;
		encoder = Charset.forName(charEncoder).newEncoder();
		encoder.reset();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;

		// create buffered output stream writer and buffers 
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
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
				maskAnalize[i] = new DataFieldParams(mask[i], map.get(mask[i]), 0);
			}
		}
		dataRecords = new LinkedList<DataRecord>();
		/*try {
			maskBytes = mask.getBytes(charSet != null ? charSet
					: Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		} catch (UnsupportedEncodingException e) {
			throw new ComponentNotReadyException(e);
		}*/
	}

    /* (non-Javadoc)
     * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
     */
    public void setDataTarget(Object out) {
        close();
        writer = (WritableByteChannel) out;
    }
    
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#close()
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
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public int writeRecord(DataRecord record) throws IOException {
        int sentBytes=0;
        int mark;
        
        sentBytes += writeString(TABLE_VERTICAL);
        
		//for each record field which is in mask change its name to value
		for (int i=0;i<maskAnalize.length;i++){
			if (dataBuffer.remaining() < fieldBuffer.limit()){
				directFlush();
			}
			//change field value to bytes
			fieldBuffer.clear();
			record.getField(maskAnalize[i].index).toByteBuffer(fieldBuffer, encoder);
			fieldBuffer.flip();
            
			blank.clear();
			blank.limit(maskAnalize[i].length - (fieldBuffer.limit()));
            mark=dataBuffer.position();

			//put field value to data buffer
			dataBuffer.put(fieldBuffer);
			dataBuffer.put(encoder.encode(blank));
            
            sentBytes+=dataBuffer.position()-mark;

            sentBytes += writeString(TABLE_VERTICAL);
		}
        sentBytes += writeString(NL);
        return sentBytes;
	}

	private int writeHeader() throws IOException {
        int sentBytes=0;
        sentBytes += writeString(TABLE_CORNER);
        for (int i=0; i<maskAnalize.length; i++) {
        	sentBytes += writeString(horizontal, maskAnalize[i].length);
            sentBytes += writeString(TABLE_CORNER);
        }
        sentBytes += writeString(NL);
        
		DataFieldMetadata[] fMetadata = metadata.getFields();
		String fName;
        sentBytes += writeString(TABLE_VERTICAL);
        for (int i=0; i<maskAnalize.length; i++) {
        	fName = fMetadata[maskAnalize[i].index].getName();
        	sentBytes += writeString(fName.getBytes());
        	sentBytes += writeString(blank, maskAnalize[i].length-fName.length());
            sentBytes += writeString(TABLE_VERTICAL);
        }
        sentBytes += writeString(NL);
        
        sentBytes += writeString(TABLE_CORNER);
        for (int i=0; i<maskAnalize.length; i++) {
        	sentBytes += writeString(horizontal, maskAnalize[i].length);
            sentBytes += writeString(TABLE_CORNER);
        }
        sentBytes += writeString(NL);

		return sentBytes;
	}
	
	private int writeFooter() throws IOException {
        int sentBytes=0;
        sentBytes += writeString(TABLE_CORNER);
        for (int i=0; i<maskAnalize.length; i++) {
        	sentBytes += writeString(horizontal, maskAnalize[i].length);
            sentBytes += writeString(TABLE_CORNER);
        }
        sentBytes += writeString(NL);

		return sentBytes;
	}
	
	private int writeString(byte[] buffer) throws IOException {
        int sentBytes=0;
        int mark;
		if (dataBuffer.remaining() < buffer.length){
			directFlush();
		}
        mark=dataBuffer.position();
        dataBuffer.put(buffer);
        sentBytes+=dataBuffer.position()-mark;
		
		return sentBytes;
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
	 * 
	 * @param record
	 * @throws IOException 
	 */
	public int write(DataRecord record) throws IOException {
		int size;
		if (dataRecords != null) {
			dataRecords.add(record.duplicate());
			if (dataRecords.size() < MAX_COUNT_ANALYZED_COUNT) {
				return 0;
			}
			analyzeRows(dataRecords, header);
			size = header ? writeHeader() : 0;
			for (DataRecord dataRecord : dataRecords) {
				size += writeRecord(dataRecord);
			}
			dataRecords = null;
			return size;
		}
		size = writeRecord(record);
		if (leftBytes > 0) {
			size += leftBytes;
			leftBytes = 0;
		}
		return size;
	}
	
	private void analyzeRows(List<DataRecord> dataRecords, boolean header) {
		int lenght;
		int max = 0;
		for (DataRecord dataRecord : dataRecords) {
			for (int i=0; i<maskAnalize.length; i++) {
				lenght = dataRecord.getField(maskAnalize[i].index).getValue().toString().length(); //getSizeSerialized()
				maskAnalize[i].length = maskAnalize[i].length < lenght ? lenght : maskAnalize[i].length;
			}
		}
		if (header) {
			DataFieldMetadata[] fMetadata = metadata.getFields();
			for (int i=0; i<maskAnalize.length; i++) {
				lenght = fMetadata[maskAnalize[i].index].getName().length();
				maskAnalize[i].length = maskAnalize[i].length < lenght ? lenght : maskAnalize[i].length;
			}
		}
		for (int i=0; i<maskAnalize.length; i++) {
			maskAnalize[i].length += PADDING_SPACE;
			rowSize += maskAnalize[i].length;
		}
		rowSize++;
		
		for (DataFieldParams dataFieldParams : maskAnalize) {
			max = max > dataFieldParams.length ? max : dataFieldParams.length;
		}
		StringBuilder sb = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		for (int i = 0; i < max; i++) {
			sb.append(' ');
			sb2.append('-');
		}
		blank = CharBuffer.wrap(sb.toString());
		horizontal = CharBuffer.wrap(sb2.toString());
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		if (dataRecords != null) {
			analyzeRows(dataRecords, header);
			leftBytes = header ? writeHeader() : 0;
			for (DataRecord dataRecord : dataRecords) {
				leftBytes += writeRecord(dataRecord);
			}
			dataRecords = null;
		}
		ByteBufferUtils.flush(dataBuffer,writer);
	}
	
	public void eof() throws IOException {
		flush();
		leftBytes += writeFooter();
		directFlush();
	}
	
	private void directFlush() throws IOException {
		ByteBufferUtils.flush(dataBuffer,writer);
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

	public void setHeader(boolean header) {
		this.header = header;
	}
	
	/**
	 * Private class for storing data field name, its andex and lenght in mask
	 */
	class DataFieldParams {
		
		String name;
		int index;
		int length;
		
		DataFieldParams(String name,int index, int length){
			this.name = name;
			this.index = index;
			this.length = length;
		}
	}

}

