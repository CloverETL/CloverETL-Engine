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
import java.util.NoSuchElementException;
import java.util.Set;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.graph.InputPort;

/**
 * Base class for streamed, i.e. non cached port data.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14.1.2014
 */
abstract class StreamedPortDataBase extends PortData {

	StreamedPortDataBase(InputPort inPort, Set<List<String>> keys) {
		super(inPort, keys);
	}

	@Override
	public void put(DataRecord record) throws IOException {}
	
	@Override
	public boolean readInputPort() {
		// streaming data, so no reading into cache
		return false;
	}
	
	static class SimpleDataIterator implements DataIterator {

		private DataRecord current;
		private DataRecord next;
		private boolean hasNext;
		private boolean allRead;
		protected InputPort inputPort;
		
		public SimpleDataIterator(InputPort inputPort) {
			this.inputPort = inputPort;
			this.next = DataRecordFactory.newRecord(inputPort.getMetadata());
		}
		
		@Override
		public boolean hasNext() throws IOException {
			fetchNextIfNeeded();
			return hasNext && !allRead;
		}

		@Override
		public DataRecord peek() {
			if (current != null) {
				return current;
			}
			throw new NoSuchElementException();
		}

		@Override
		public DataRecord next() throws IOException {
			fetchNextIfNeeded();
			if (hasNext) {
				if (current == null) {
					current = next.duplicate();
				} else {
					current.copyFrom(next);
				}
				hasNext = false;
				return current;
			} else {
				throw new NoSuchElementException();
			}
		}
		
		protected void fetchNextIfNeeded() throws IOException {
			if (hasNext || allRead) {
				return;
			}
			if (fetchNext(next) == null) {
				/* end of data reached, clear all */
				hasNext = false;
				allRead = true;
				current = null;
				next = null;
			} else {
				hasNext = true;
			}
		}
		
		protected DataRecord fetchNext(DataRecord target) throws IOException {
			try {
				return inputPort.readRecord(target);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}
}
