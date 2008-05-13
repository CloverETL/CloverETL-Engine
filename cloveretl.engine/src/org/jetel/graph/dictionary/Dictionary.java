/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.graph.dictionary;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;

public class Dictionary extends GraphElement {

	private static final String DEFAULT_ID = "_DICTIONARY";

	private Map<String, IDictionaryValue<?>> dictionary;
	private Map<String, IDictionaryValue<?>> defaultDictionary;

	public Dictionary(TransformationGraph graph) {
		super(DEFAULT_ID, graph);
		
		dictionary = new HashMap<String, IDictionaryValue<?>>();
		defaultDictionary = new HashMap<String, IDictionaryValue<?>>();

	}

	@Override
	public synchronized void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

		//initialization of all default dictionary values
		for(IDictionaryValue<?> dictionaryValue : defaultDictionary.values()) {
			dictionaryValue.init(this);
		}
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
//TODO !!!		//dictionary.clear();
	}
	
	public IDictionaryValue<?> get(String key) {
		if(dictionary.containsKey(key)) {
			return dictionary.get(key);
		} else {
			return defaultDictionary.get(key);
		}
	}
	
	public void put(String key, IDictionaryValue<?> value) {
		//value.init(this);
		dictionary.put(key, value);
	}
	
	public void putDefault(String key, IDictionaryValue<?> value) {
		defaultDictionary.put(key, value);
	}

	public Set<String> getKeys() {
		Set<String> result = new HashSet<String>();
		result.addAll(dictionary.keySet());
		result.addAll(defaultDictionary.keySet());
		return result;
	}
	
	public boolean isEmpty() {
		return dictionary.isEmpty() && defaultDictionary.isEmpty();
	}
}
