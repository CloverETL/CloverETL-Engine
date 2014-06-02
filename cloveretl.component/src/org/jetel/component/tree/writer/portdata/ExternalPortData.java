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
package org.jetel.component.tree.writer.portdata;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;
import jdbm.helper.Tuple;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12 Sep 2011
 */
abstract class ExternalPortData extends PortData {
	
	private static final byte[] SERIALIZED_LONG_ZERO = toByteArray(0L);

	protected static final int PAGE_SIZE = 64;
	protected static final int SERIALIZED_COUNTER_LENGTH = 8;
	
	protected EntrySerializer serializer;
	protected RecordKeyComparator recordKeyComparator;

	protected String fileName;

	protected CloverBuffer recordBuffer;
	protected long keyCounter = 0;

	protected CacheRecordManager sharedCache;
	
	ExternalPortData(InputPort inPort, Set<List<String>> keys) {
		super(inPort, keys);
	}

	@Override
	public void setSharedCache(CacheRecordManager sharedCache) {
		this.sharedCache = sharedCache;
	}

	@Override
	public boolean readInputPort() {
		return true;
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		serializer = new EntrySerializer();
		recordKeyComparator = new RecordKeyComparator();
	}

	protected byte[] getDatabaseKey(DataRecord record, int[] key, DataRecord parentRecord, int[] parentKey) {
		for (int i = 0; i < parentKey.length; i++) {
			record.getField(key[i]).setValue(parentRecord.getField(parentKey[i]));
		}
		
		record.serializeUnitary(recordBuffer, key);
		recordBuffer.put(SERIALIZED_LONG_ZERO);
		int dataLength = recordBuffer.position();
		recordBuffer.flip();
		byte[] serializedKey = new byte[dataLength];
		recordBuffer.get(serializedKey);
		recordBuffer.clear();

		return serializedKey;
	} 
		
	protected static boolean equalsKey(Tuple<byte[], byte[]> tuple, byte[] key) {
		byte[] foundKey = tuple.getKey();
		
		if (key.length != foundKey.length) {
			return false;
		}
		for (int i = 0; i < key.length - SERIALIZED_COUNTER_LENGTH; i++) {
			if (foundKey[i] != key[i]) {
				return false;
			}
		}
		return true;
	}
	
	protected static byte[] toByteArray(long data) {
		return new byte[] {
	        (byte)((data >> 56) & 0xff),
	        (byte)((data >> 48) & 0xff),
	        (byte)((data >> 40) & 0xff),
	        (byte)((data >> 32) & 0xff),
	        (byte)((data >> 24) & 0xff),
	        (byte)((data >> 16) & 0xff),
	        (byte)((data >> 8) & 0xff),
	        (byte)(data & 0xff),
	    };
	}
	
	protected static long fromByteArray(byte[] data, int pos) {
		return (((long) (data[pos + 0] & 0xff) << 56) |
				((long) (data[pos + 1] & 0xff) << 48) |
				((long) (data[pos + 2] & 0xff) << 40) |
				((long) (data[pos + 3] & 0xff) << 32) |
				((long) (data[pos + 4] & 0xff) << 24) |
				((long) (data[pos + 5] & 0xff) << 16) |
				((long) (data[pos + 6] & 0xff) << 8) |
				((long) (data[pos + 7] & 0xff)));
	}

	private static class EntrySerializer implements Serializer<byte[]> {

		@Override
		public void serialize(SerializerOutput out, byte[] obj) throws IOException {
			out.write(obj);
		}

		@Override
		public byte[] deserialize(SerializerInput in) throws IOException, ClassNotFoundException {
			byte[] data = new byte[in.available()];
			in.read(data);
			return data;
		}
	}

	private static class RecordKeyComparator implements Comparator<byte[]>, Serializable {
		private static final long serialVersionUID = -8678623124738984233L;

		@Override
		public int compare(byte[] key1, byte[] key2) {
			if (key1 == null) {
				throw new IllegalArgumentException("Argument 'key1' is null");
			}
			if (key2 == null) {
				throw new IllegalArgumentException("Argument 'key2' is null");
			}
			
			if (key1.length != key2.length) {
				return key1.length - key2.length;
			}
			for (int i = 0; i < key1.length - SERIALIZED_COUNTER_LENGTH; i++) {
				if (key1[i] != key2[i]) {
					return key1[i] - key2[i]; 
				}
			}
			long result = fromByteArray(key1, key1.length - 8) - fromByteArray(key2, key2.length - 8); 
			return result > 0 ? 1 : result < 0 ? -1 : 0;
		}
	}
}
