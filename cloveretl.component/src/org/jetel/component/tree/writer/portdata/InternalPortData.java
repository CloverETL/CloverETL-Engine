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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;

/**
 * Port data stored in memory.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.8.2013
 */
abstract class InternalPortData extends PortData {

	protected DataRecord keyRecord;
	
	InternalPortData(InputPort inPort, Set<List<String>> keys) {
		super(inPort, keys);
	}

	@Override
	public boolean readInputPort() {
		return true;
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		keyRecord = DataRecordFactory.newRecord(inPort.getMetadata());
	}
	
	@Override
	public void free() {
		super.free();
		keyRecord = null;
	}
	
	@Override
	public DataIterator iterator(int key[], int parentKey[], DataRecord keyData, DataRecord nextKeyData)
			throws IOException {
		
		Collection<DataRecord> data = fetchData(key, parentKey, keyData);
		if (data != null) {
			return new DelegatingDataIterator(data.iterator());
		}
		return EmptyDataIterator.INSTANCE;
	}
	
	protected abstract Collection<DataRecord> fetchData(int key[], int parentKey[], DataRecord keyData);
	
	static final class EmptyDataIterator implements DataIterator {

		static final EmptyDataIterator INSTANCE = new EmptyDataIterator();
		
		private EmptyDataIterator() {}
		
		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public DataRecord peek() {
			throw new NoSuchElementException();
		}

		@Override
		public DataRecord next() throws IOException {
			throw new NoSuchElementException();
		}
	}
	
	static class DelegatingDataIterator implements DataIterator {

		Iterator<DataRecord> delegate;
		DataRecord current;
		
		DelegatingDataIterator(Iterator<DataRecord> recordIterator) {
			this.delegate = recordIterator;
		}
		
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public DataRecord peek() {
			if (current == null) {
				throw new NoSuchElementException();
			}
			return current;
		}

		@Override
		public DataRecord next() throws IOException {
			current = null;
			current = delegate.next();
			return current;
		}
	}
}
