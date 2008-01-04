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
import java.util.Map;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;

public class Dictionary extends GraphElement {

	private static final String DEFAULT_ID = "_DICTIONARY";
	
	public Dictionary(TransformationGraph graph) {
		super(DEFAULT_ID, graph);
	}

	private Map<String, DictionaryValue<?>> dictionary;
	private Map<String, DictionaryValue<?>> defaultDictionary;
	
	@Override
	public synchronized void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		dictionary = new HashMap<String, DictionaryValue<?>>();
		defaultDictionary = new HashMap<String, DictionaryValue<?>>();
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
		dictionary.clear();
	}
	
	public DictionaryValue<?> get(String key) {
		if(dictionary.containsKey(key)) {
			return dictionary.get(key);
		} else {
			return defaultDictionary.get(key);
		}
	}
	
	public void put(String key, DictionaryValue<?> value) {
		dictionary.put(key, value);
	}
	
	public void putDefault(String key, DictionaryValue<?> value) {
		defaultDictionary.put(key, value);
	}
	
}
