
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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.string.StringUtils;

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
public class StructureFormatter implements Formatter {
	
	private DataRecordMetadata metadata;
	private WritableByteChannel writer;
	private String mask;
	private byte[] maskBytes;
	private DataFieldParams[] maskAnalize;
	private int[] fieldIndexes;
	private int index;
	private int lastIndex;
	private int fieldIndex;
	private ByteBuffer fieldBuffer; 
	private ByteBuffer dataBuffer;
	private CharsetEncoder encoder;
	private String charSet = null;
	private String sFooter; 
	private String sHeader; 
	private ByteBuffer footer; 
	private ByteBuffer header; 
	

	/**
	 * Constructor without parameters
	 */
	public StructureFormatter(){
		charSet = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;
	}
	
	/**
	 * Constructor
	 * 
	 * @param charEncoder charset for coding characters
	 */
	public StructureFormatter(String charEncoder){
		charSet = charEncoder;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;
		encoder = Charset.forName(charSet).newEncoder();
		encoder.reset();

		// create buffered output stream writer and buffers 
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
		//if mask is not given create default mask
		if (mask == null) {
			StringBuilder maskBuilder = new StringBuilder();
			maskBuilder.append("< ");
			maskBuilder.append(metadata.getName());
			maskBuilder.append(" ");
			for (int i=0;i<metadata.getNumFields();i++){
				maskBuilder.append(metadata.getField(i).getName());
				maskBuilder.append("=$");
				maskBuilder.append(metadata.getField(i).getName());
				maskBuilder.append(" ");
			}
			maskBuilder.append("/>\n");
			mask = maskBuilder.toString();
		}
		try {
			maskBytes = mask.getBytes(charSet != null ? charSet
					: Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		} catch (UnsupportedEncodingException e) {
			throw new ComponentNotReadyException(e);
		}

		//find places in mask where to put record field's values
		List<DataFieldParams> maskAnalizeMap = new ArrayList<DataFieldParams>();
		String fieldName;
		int index ;
		int startIndex = 0;
		Map fields = metadata.getFieldNamesMap();
		do {
			//find next '$' in mask
			index = mask.indexOf('$',startIndex);
			if (index > -1){//if found
				//find end of field name
				startIndex = StringUtils.findIdentifierEnd(mask,index + 1);
				fieldName = mask.substring(index + 1,startIndex);
				//if substring after '$' is a field name from metadata add it (with index in mask and its length) to list
				if (fields.containsKey(fieldName)){
					maskAnalizeMap.add(new DataFieldParams(fieldName,index, startIndex - index));
				}
			}
		}while (index >-1);
		//change list to array
		maskAnalize = new DataFieldParams[maskAnalizeMap.size()];
		maskAnalizeMap.toArray(maskAnalize);
		fieldIndexes = new int[maskAnalize.length];
		for (int i = 0; i < fieldIndexes.length; i++) {
			fieldIndexes[i] = (Integer)fields.get(maskAnalize[i].name);
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
		dataBuffer.clear();
		fieldBuffer.clear();
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
	public int write(DataRecord record) throws IOException {
		lastIndex = 0;
        int sentBytes=0;
        int mark;
		//for each record field which is in mask change its name to value
		int i = 0;
		try {
			for (i=0;i<maskAnalize.length;i++){
				fieldIndex = fieldIndexes[i];
				index  = maskAnalize[i].index;
				if (dataBuffer.remaining() < index - lastIndex){
					flush();
				}
				//put bytes from mask from last field name to actual one to buffer
	            mark=dataBuffer.position();
				dataBuffer.put(maskBytes, lastIndex, index - lastIndex);
				fieldBuffer.clear();
				//change field value to bytes
				record.getField(fieldIndex).toByteBuffer(fieldBuffer, encoder);
				fieldBuffer.flip();
	            sentBytes+=dataBuffer.position()-mark;
				if (dataBuffer.remaining() < fieldBuffer.limit()){
					flush();
				}
	            mark=dataBuffer.position();
				//put field value to data buffer
				dataBuffer.put(fieldBuffer);
	            sentBytes+=dataBuffer.position()-mark;
				//set processed part of mask to the end of field name identifier
				lastIndex = index + maskAnalize[i].length;
			}
			//put rest of mask (after last data field) to data buffer
			if (dataBuffer.remaining() < maskBytes.length - lastIndex){
				flush();
			}
	        mark=dataBuffer.position();
			dataBuffer.put(maskBytes, lastIndex, maskBytes.length - lastIndex);
	        sentBytes+=dataBuffer.position()-mark;
		} catch (CharacterCodingException e) {
            throw new RuntimeException("Exception when converting the field value: " + record.getField(i).getValue() + " (field name: '" + record.getMetadata().getField(i).getName() + "') to " + encoder.charset() + ". (original cause: " + e.getMessage() + ") \n\nRecord: " +record.toString(), e);
		}
        
        return sentBytes;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		ByteBufferUtils.flush(dataBuffer,writer);
	}

	public void setMask(String mask) {
		this.mask = mask;
	}

	/**
	 * Returns name of charset which is used by this formatter
	 * @return Name of charset or null if none was specified
	 */
	public String getCharsetName() {
		return(this.charSet);
	}

	/**
	 * Private class for storing data field name, its andex and lenght in mask
	 */
	private static class DataFieldParams {
		
		String name;
		int index;
		int length;
		
		DataFieldParams(String name,int index, int length){
			this.name = name;
			this.index = index;
			this.length = length;
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

