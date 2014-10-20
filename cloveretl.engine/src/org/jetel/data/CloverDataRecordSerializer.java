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

import org.jetel.util.bytes.CloverBuffer;

/**
 * {@link DataRecordSerializer} that uses internal {@link DataField} serialization. 
 *  
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17. 10. 2014
 */
public class CloverDataRecordSerializer implements DataRecordSerializer {
	
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
		field.serialize(buffer);
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
		field.serialize(buffer);
	}

	@Override
	public void serialize(CloverBuffer buffer, MapDataField field) {
		field.serialize(buffer);
	}

	

	@Override
	public void deserialize(CloverBuffer buffer, StringDataField field) {
		field.deserialize(buffer);
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
		field.deserialize(buffer);
	}

	@Override
	public void deserialize(CloverBuffer buffer, MapDataField field) {
		field.deserialize(buffer);
	}
	
}
