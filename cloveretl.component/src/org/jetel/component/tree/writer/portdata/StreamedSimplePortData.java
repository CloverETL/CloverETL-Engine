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
import java.util.List;
import java.util.Set;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.graph.InputPort;

/**
 * Simple streamed port data, that cannot be looked-up by a key. Provides optional
 * check of data sorting.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14.1.2014
 */
class StreamedSimplePortData extends StreamedPortDataBase {

	protected SortHint sortHint;
	
	StreamedSimplePortData(InputPort inPort, Set<List<String>> keys, SortHint sortHint) {
		super(inPort, keys);
		this.sortHint = sortHint;
	}
	
	@Override
	public DataIterator iterator(int[] key, int[] parentKey, DataRecord keyData, DataRecord nextKeyData)
			throws IOException {
		if (sortHint != null) {
			return new SortCheckDataIterator(inPort, sortHint);
		} else {
			return new SimpleDataIterator(inPort);
		}
	}
	
	static class SortCheckDataIterator extends SimpleDataIterator {
		
		private SortHint sortHint;
		private int sortIndices[];
		private DataRecord prev;
		
		public SortCheckDataIterator(InputPort inputPort, SortHint sortHint) {
			super(inputPort);
			this.sortHint = sortHint;
			this.sortIndices = inputPort.getMetadata().fieldsIndices(sortHint.getKeyFields());
		}
		
		@Override
		protected DataRecord fetchNext(DataRecord target) throws IOException {
			
			DataRecord next = doFetchNext(target);
			if (next == null) {
				prev = null;
				return null;
			}
			if (next != null && prev != null) {
				checkOrder(prev, next);
			}
			prev = next.duplicate();
			return next;
		}
		
		protected DataRecord doFetchNext(DataRecord target) throws IOException {
			return super.fetchNext(target);
		}
		
		private void checkOrder(DataRecord prev, DataRecord next) throws IOException {
			
			for (int i = 0; i < sortIndices.length; ++i) {
				DataField prevField = prev.getField(sortIndices[i]);
				DataField nextField = next.getField(sortIndices[i]);
				if (prevField.isNull() && nextField.isNull()) {
					continue;
				}
				int diff = sortHint.getAscending()[i] ? nextField.compareTo(prevField) : prevField.compareTo(nextField);
				if (diff < 0) {
					throw new IOException("Input data records are not sorted on input port "+ inputPort.getInputPortNumber()
							+ ". In record #" + (inputPort.getInputRecordCounter()) 
							+ ", key field \"" + sortHint.getKeyFields()[i]+"\""
							+ ", value \""  + nextField.toString() + "\""
							+ " is " + (sortHint.getAscending()[i] ? "less" : "greater")
							+ " than previous value \"" + prevField.toString() + "\".");
				}
			}
		}
	}
}
