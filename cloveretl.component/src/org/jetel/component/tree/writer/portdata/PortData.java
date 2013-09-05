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

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Abstract class representing data provider wrapping input port for xml writer model.
 * Reflects components life-cycle.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 20 Dec 2010
 */
public abstract class PortData {
	
	/** input port to read data from */
	protected InputPort inPort;
	/** array containing all keys data can be looked up under */
	protected int[][] primaryKey;
	/** flag which means that data can be looked up without any key - e.g. iterator should return all the data */
	protected boolean nullKey;
	
	/**
	 * Factory method
	 * 
	 * @param cached true if all records should be cached first
	 * @param inPort input port to read records from
	 * @param keys the keys under which records can be looked up 
	 * @param hint 
	 * @param tempDirectory
	 * @param cacheSize
	 * @return
	 */
	public static PortData getInstance(boolean cached, boolean inMemory, InputPort inPort, Set<List<String>> keys,
			SortHint hint) {
		if (cached) {
			if (inMemory) {
				if (isSingleFieldKey(keys)) {
					return new InternalSimplePortData(inPort, keys);
				} else {
					return new InternalComplexPortData(inPort, keys);
				}
			} else {
				if (keys.size() > 1) {
					return new ExternalComplexPortData(inPort, keys);
				} else {
					return new ExternalSimplePortData(inPort, keys);
				}
			}
		} else {
			return new StreamedPortData(inPort, keys, hint);
		}
	}
	
	private static boolean isSingleFieldKey(Set<List<String>> keys) {
		
		if (keys.size() == 1) {
			List<String> fieldList = keys.iterator().next();
			if (fieldList != null && fieldList.size() == 1) {
				return true;
			}
		}
		return false;
	}
	
	PortData(InputPort inPort, Set<List<String>> keys) {
		this.inPort = inPort;
		DataRecordMetadata metadata = inPort.getMetadata();
		
		nullKey = keys.contains(null);
		while (keys.remove(null));
		
		primaryKey = new int[keys.size()][];
		int outer = 0;
		for (List<String> key : keys) {
			primaryKey[outer++] = resolveKey(metadata, key);
		}
	}
	
	public void init() throws ComponentNotReadyException {
	}
	
	public void preExecute() throws ComponentNotReadyException {
	}
	
	public void postExecute() throws ComponentNotReadyException {		
	}
	
	public void free() {		
	}
	
	public void setSharedCache(CacheRecordManager sharedCache) {
		//ignore
	}
	
	public abstract boolean readInputPort();
	
	public abstract void put(DataRecord record) throws IOException;

	public abstract DataIterator iterator(int[] key, int[] parentKey, DataRecord keyData, DataRecord nextKeyData) throws IOException;
	
	public InputPort getInPort() {
		return inPort;
	}
	
	private int[] resolveKey(DataRecordMetadata metadata, List<String> key) {
		int[] resolvedKey = new int[key.size()];
		for (int i = 0; i < key.size(); i++) {
			resolvedKey[i] = metadata.getFieldPosition(key.get(i));
		}
		return resolvedKey;
	}
}
