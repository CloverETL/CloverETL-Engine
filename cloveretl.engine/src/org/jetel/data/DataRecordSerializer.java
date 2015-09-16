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
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 6, 2014
 */
public interface DataRecordSerializer {
		public abstract void serialize(CloverBuffer buffer, DataRecord record);
		
		public abstract void serialize(CloverBuffer buffer, DataRecord record, int[] whichFields);
		
		public abstract void deserialize(CloverBuffer buffer, DataRecord record);
		
		public abstract void deserialize(CloverBuffer buffer, DataRecord record, int[] whichFields);
		
		
		public abstract void serialize(CloverBuffer buffer, StringDataField field);
		public abstract void serialize(CloverBuffer buffer, IntegerDataField field);
		public abstract void serialize(CloverBuffer buffer, LongDataField field);
		public abstract void serialize(CloverBuffer buffer, NumericDataField field);
		public abstract void serialize(CloverBuffer buffer, DecimalDataField field);
		public abstract void serialize(CloverBuffer buffer, BooleanDataField field);
		public abstract void serialize(CloverBuffer buffer, DateDataField field);
		public abstract void serialize(CloverBuffer buffer, ByteDataField field);
		public abstract void serialize(CloverBuffer buffer, CompressedByteDataField field);
		public abstract void serialize(CloverBuffer buffer, ListDataField field);
		public abstract void serialize(CloverBuffer buffer, MapDataField field);
		
		public abstract void deserialize(CloverBuffer buffer, StringDataField field);
		public abstract void deserialize(CloverBuffer buffer, IntegerDataField field);
		public abstract void deserialize(CloverBuffer buffer, LongDataField field);
		public abstract void deserialize(CloverBuffer buffer, NumericDataField field);
		public abstract void deserialize(CloverBuffer buffer, DecimalDataField field);
		public abstract void deserialize(CloverBuffer buffer, BooleanDataField field);
		public abstract void deserialize(CloverBuffer buffer, DateDataField field);
		public abstract void deserialize(CloverBuffer buffer, ByteDataField field);
		public abstract void deserialize(CloverBuffer buffer, CompressedByteDataField field);
		public abstract void deserialize(CloverBuffer buffer, ListDataField field);
		public abstract void deserialize(CloverBuffer buffer, MapDataField field);
}
