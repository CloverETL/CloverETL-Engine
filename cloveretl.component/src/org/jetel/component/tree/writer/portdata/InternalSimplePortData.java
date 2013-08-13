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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.map.MultiValueMap;
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
	
	InternalSimplePortData(InputPort inPort, Set<List<String>> keys) {
		super(inPort, keys);
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		if (nullKey) {
			/*
			 * lookup with null key should return elements in insertion
			 * order - so replacing default HashMap with LinkedHashMap
			 */
			records = MultiValueMap.decorate(new LinkedHashMap<Object, Object>(), LinkedList.class);
		}
	}

	@Override
	public void put(DataRecord record) throws IOException {
		records.put(record.getField(primaryKey[0][0]), record);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Collection<DataRecord> fetchData(int key[], int parentKey[], DataRecord keyData) {
		if (key == null) {
			return records.values();
		} else {
			return records.getCollection(keyData.getField(parentKey[0]));
		}
	}
}
