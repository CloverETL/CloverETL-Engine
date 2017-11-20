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
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;

/**
 * Internal (i.e. in memory) port data that are looked-up
 * just by one {@link DataField}.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13.8.2013
 */
class InternalSimplePortData extends InternalPortData {
	
	private MultiValuedMap<DataField, DataRecord> records;

	InternalSimplePortData(InputPort inPort, Set<List<String>> keys) {
		super(inPort, keys);
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		records = new ArrayListValuedHashMap<>();
	}
	
	@Override
	public void free() {
		super.free();
		records = null;
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		records.clear();
	}

	@Override
	public void put(DataRecord record) throws IOException {
		if (nullKey) {
			records.put(null, record);
		}
		if (primaryKey.length == 1) {
			records.put(record.getField(primaryKey[0][0]), record);
		}
	}

	@Override
	protected Collection<DataRecord> fetchData(int key[], int parentKey[], DataRecord parentData) {
		if (key == null) {
			return (Collection<DataRecord>)records.get(null);
		} else {
			DataField childKeyField = keyRecord.getField(key[0]).duplicate();
			childKeyField.reset();
			childKeyField.setValue(parentData.getField(parentKey[0]));
			Collection<DataRecord> data = records.get(childKeyField);
			return data;
		}
	}
}
