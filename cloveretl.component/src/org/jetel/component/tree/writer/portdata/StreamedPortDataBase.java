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
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.InputPort;

/**
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
	
	class SimpleDataIterator implements DataIterator {

		protected DataRecord current;
		protected DataRecord next;
		protected boolean hasNext;
		protected boolean allRead;
		
		public SimpleDataIterator() {
			this.next = DataRecordFactory.newRecord(inPort.getMetadata());
			this.next.init();
		}
		
		@Override
		public boolean hasNext() {
			try {
				fetchNextIfNeeded();
			} catch (IOException e) {
				throw new JetelRuntimeException(e);
			}
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
			if (!fetchNext(next)) {
				/* end of data reached, clear all */
				hasNext = false;
				allRead = true;
				current = null;
				next = null;
			} else {
				hasNext = true;
			}
		}
		
		protected boolean fetchNext(DataRecord target) throws IOException {
			try {
				 target = inPort.readRecord(target);
				 return target != null;
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}
}
