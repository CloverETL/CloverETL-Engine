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
package org.jetel.data;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Map.Entry;


import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.bytes.DynamicCloverBuffer;
import org.jetel.util.bytes.UTF8CloverBufferReader;
import org.jetel.util.bytes.UTF8CloverBufferWriter;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 6, 2014
 */
public class CloverDataRecordSerializer implements DataRecordSerializer {
	
	UTF8CloverBufferWriter utfWriter;
	UTF8CloverBufferReader utfReader;
	
	public CloverDataRecordSerializer(){
		utfWriter = new UTF8CloverBufferWriter();
		utfReader = new UTF8CloverBufferReader();
	}
	
	
	@Override
	public void serialize(CloverBuffer buffer, DataRecord record) {
		final DataField[] fields=record.getFields();
		for(int i=0;i<fields.length;i++){
			fields[i].serialize(buffer, this);
		}
	}

	@Override
	public void serialize(CloverBuffer buffer, DataRecord record, int[] whichFields) {
		final DataField[] fields=record.getFields();
		for(int i:whichFields){
			fields[i].serialize(buffer,this);
        }
	}
	
	
	@Override
	public void deserialize(CloverBuffer buffer, DataRecord record) {
		final DataField[] fields=record.getFields();
		for(int i=0;i<fields.length;i++){
			fields[i].deserialize(buffer, this);
		}
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, DataRecord record, int[] whichFields) {
		final DataField[] fields=record.getFields();
		for(int i:whichFields){
			fields[i].deserialize(buffer,this);
        }
		
	}
	

	@Override
	public void serialize(CloverBuffer buffer, StringDataField field) {
		try {
			// encode nulls as zero, increment length of non-null values by one
			ByteBufferUtils.encodeLength(buffer,field.isNull() ? 0 : field.length() + 1);
			
			utfWriter.setOutput(buffer);
			utfWriter.write(field);
			utfWriter.reset();
			
    	} catch (BufferOverflowException | IOException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
		

	}

	@Override
	public void serialize(CloverBuffer buffer, IntegerDataField field) {
		field.serialize(buffer);

	}

	@Override
	public void serialize(CloverBuffer buffer, LongDataField field) {
		field.serialize(buffer);

	}

	@Override
	public void serialize(CloverBuffer buffer, NumericDataField field) {
		field.serialize(buffer);

	}

	@Override
	public void serialize(CloverBuffer buffer, DecimalDataField field) {
		field.serialize(buffer);

	}

	@Override
	public void serialize(CloverBuffer buffer, BooleanDataField field) {
		field.serialize(buffer);

	}

	@Override
	public void serialize(CloverBuffer buffer, DateDataField field) {
		field.serialize(buffer);

	}

	@Override
	public void serialize(CloverBuffer buffer, ByteDataField field) {
		field.serialize(buffer);

	}

	@Override
	public void serialize(CloverBuffer buffer, CompressedByteDataField field) {
		field.serialize(buffer);

	}

	@Override
	public void serialize(CloverBuffer buffer, ListDataField field) {
		try {
			// encode null as zero, increment size of non-null values by one
			ByteBufferUtils.encodeLength(buffer, field.isNull ? 0 : field.getSize() + 1);

			for (DataField lfield : field) {
				lfield.serialize(buffer,this);
			}
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
    	}

	}

	@Override
	public void serialize(CloverBuffer buffer, MapDataField field) {
		try {
			// encode null as zero, increment size of non-null values by one
			ByteBufferUtils.encodeLength(buffer, field.isNull() ? 0 : field.getSize() + 1);

			for (Entry<String, DataField> fieldEntry : field.getFields()) {
				encodeString(buffer, fieldEntry.getKey());
				fieldEntry.getValue().serialize(buffer,this);
			}
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
	}

	

	@Override
	public void deserialize(CloverBuffer buffer, StringDataField field) {
		// encoded length is incremented by one, decrement it back to normal
		final int length = ByteBufferUtils.decodeLength(buffer) - 1;
		
		// empty value - so we can store new string
		field.value.setLength(0);

		if (length < 0) {
			field.setNull(true);
		} else {
			try {
		
				utfReader.setInput(buffer);
				utfReader.read(field,length);
				utfReader.reset();
				
			} catch (IOException e) {
				throw new RuntimeException("Exception during deserializing StringDataField: " + field.getMetadata().getName(), e);
			}
			field.setNull(false);
		}

	}

	@Override
	public void deserialize(CloverBuffer buffer, IntegerDataField field) {
		field.deserialize(buffer);
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, LongDataField field) {
		field.deserialize(buffer);
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, NumericDataField field) {
		field.deserialize(buffer);
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, DecimalDataField field) {
		field.deserialize(buffer);
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, BooleanDataField field) {
		field.deserialize(buffer);
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, DateDataField field) {
		field.deserialize(buffer);
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, ByteDataField field) {
		field.deserialize(buffer);
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, CompressedByteDataField field) {
		field.deserialize(buffer);
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, ListDataField field) {
		// encoded length is incremented by one, decrement it back to normal
		final int length = ByteBufferUtils.decodeLength(buffer) - 1;

		// clear the list
		field.clear();

		if (length == -1) {
			field.setNull(true);
		} else {
			for (int i = 0; i < length; i++) {
				field.addField().deserialize(buffer,this);
			}
			field.setNull(false);
		}
		
	}

	@Override
	public void deserialize(CloverBuffer buffer, MapDataField field) {
		// encoded length is incremented by one, decrement it back to normal
		final int length = ByteBufferUtils.decodeLength(buffer) - 1;

		// clear the list
		field.clear();

		if (length == -1) {
			field.setNull(true);
		} else {
			for (int i = 0; i < length; i++) {
				field.putField(decodeString(buffer).toString()).deserialize(buffer, this);
			}
			field.setNull(false);
		}

	}
	
	 public void encodeString(CloverBuffer buffer, CharSequence str) {
			// encode nulls as zero, increment length of non-null values by one
	    	if (str == null) {
	    		ByteBufferUtils.encodeLength(buffer, 0);
	    	} else {
	    		final int length = str.length();
				ByteBufferUtils.encodeLength(buffer, length + 1);
				
				try {
					utfWriter.setOutput(buffer);
					utfWriter.write(str);
					utfWriter.reset();
				} catch (IOException e) {
			    	throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
				}
	    	}
	    }
	 
	public CharSequence decodeString(CloverBuffer buffer) {
		int length = ByteBufferUtils.decodeLength(buffer)-1;
		if (length == 0) {
			return null;
		} else {
			StringBuilder sb = new StringBuilder(length);
			try {

				utfReader.setInput(buffer);
				utfReader.read(sb, length);
				utfReader.reset();
			} catch (IOException e) {
				throw new RuntimeException("Exception during deserializing string value", e);
			}
			return sb;
		}
	}

	public static void main(String[] args){
		String test="Jakobysenechumelilo +ěščřžýáíé😄⧯⧰⧭⧬不丰互五卅𠀋㐂両丧並䶵";
		StringDataField field = new StringDataField(new DataFieldMetadata("test", DataFieldMetadata.STRING_FIELD, ";"));
		field.setValue(test);
		System.err.println(test);
		CloverBuffer buffer = DynamicCloverBuffer.allocate(512);
		CloverDataRecordSerializer serializer = new CloverDataRecordSerializer();
		field.serialize(buffer, serializer);
		field.reset();
		buffer.flip();
		field.deserialize(buffer, serializer);
		System.err.println(field.toString());
		buffer.clear();
		serializer.encodeString(buffer, test);
		buffer.flip();
		System.err.println(serializer.decodeString(buffer));
		
	}
	
}
