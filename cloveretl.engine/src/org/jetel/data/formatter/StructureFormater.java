
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
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ByteBufferUtils;
import org.jetel.util.StringUtils;

/**
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 27, 2006
 *
 */
public class StructureFormater implements Formatter {
	
	private DataRecordMetadata metadata;
	private WritableByteChannel writer;
	private String mask;
	private byte[] maskBytes;
	private DataFieldParams[] maskAnalize;
	private int index;
	private int lastIndex;
	private String fieldName;
	private ByteBuffer fieldBuffer; 
	private ByteBuffer dataBuffer;
	private CharsetEncoder encoder;
	private String charSet = null;
	
	private static final int INDEX = 0;
	private static final int LENGHT = 0;
	

	public StructureFormater(){
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
		encoder = Charset.forName(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER).newEncoder();
		encoder.reset();
	}
	
	public StructureFormater(String charEncoder){
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
		charSet = charEncoder;
		encoder = Charset.forName(charEncoder).newEncoder();
		encoder.reset();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object out, DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;

		// create buffered output stream reader 
		writer = (WritableByteChannel) out;
		
		maskBytes = mask.getBytes();

		List<DataFieldParams> maskAnalizeMap = new ArrayList<DataFieldParams>();
		String fieldName;
		int index ;
		int startIndex = 0;
		Map fields = metadata.getFieldNames();
		do {
			index = mask.indexOf('$',startIndex);
			if (index > -1){
				startIndex = StringUtils.findIdentifierEnd(mask,index + 1);
				fieldName = mask.substring(index + 1,startIndex);
				if (fields.containsKey(fieldName)){
					maskAnalizeMap.add(new DataFieldParams(fieldName,index, startIndex - index));
				}
			}
		}while (index >-1);
		maskAnalize = new DataFieldParams[maskAnalizeMap.size()];
		maskAnalizeMap.toArray(maskAnalize);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	public void close() {
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
	public void write(DataRecord record) throws IOException {
		lastIndex = 0;
		for (int i=0;i<maskAnalize.length;i++){
			fieldName = maskAnalize[i].name;
			index  = maskAnalize[i].index;
			if (dataBuffer.remaining() < index - lastIndex){
				flush();
			}
			dataBuffer.put(maskBytes, lastIndex, index - lastIndex);
			fieldBuffer.clear();
			record.getField(fieldName).toByteBuffer(fieldBuffer, encoder);
			fieldBuffer.flip();
			if (dataBuffer.remaining() < fieldBuffer.limit()){
				flush();
			}
			dataBuffer.put(fieldBuffer);
			lastIndex = index + maskAnalize[i].length;
		}
		if (dataBuffer.remaining() < maskBytes.length - lastIndex){
			flush();
		}
		dataBuffer.put(maskBytes, lastIndex, maskBytes.length - lastIndex);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		ByteBufferUtils.flush(dataBuffer,writer);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#setOneRecordPerLinePolicy(boolean)
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
	}

	public void setMask(String mask) {
		this.mask = mask;
	}

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

