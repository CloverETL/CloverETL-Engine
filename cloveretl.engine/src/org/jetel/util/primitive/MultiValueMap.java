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
package org.jetel.util.primitive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is simple wrapper around a map passed in constructor. The map 
 * has list of values stored under a key. The regular approach how multiple
 * values can be stored in Map is used in this class and can be extended in the future.
 * 
 * For now the most important method is {@link #putValue(Object, Object)}.
 * 
 * This class is more consistent substitution for {@link DuplicateKeyMap} class.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.9.2012
 * @see DuplicateKeyMap
 */
public class MultiValueMap<K, V> implements Map<K, List<V>> {

	private Map<K, List<V>> innerMap;
	
	/**
	 * The only constructor which wraps the given map by this MultiValueMap.
	 */
	public MultiValueMap(Map<K, List<V>> innerMap) {
		if (innerMap == null) {
			throw new NullPointerException("inner map is null");
		}
		this.innerMap = innerMap;
	}
	
	@Override
	public int size() {
		return innerMap.size();
	}

	@Override
	public boolean isEmpty() {
		return innerMap.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return innerMap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return innerMap.containsValue(value);
	}

	@Override
	public List<V> get(Object key) {
		return innerMap.get(key);
	}

	@Override
	public List<V> put(K key, List<V> value) {
		return innerMap.put(key, value);
	}

	/**
	 * Put the given value to a list stored under the given key.
	 */
	public void putValue(K key, V value) {
		List<V> list = innerMap.get(key);
		if (list == null) {
			list = new ArrayList<V>();
			list.add(value);
			innerMap.put(key, list);
		} else {
			list.add(value);
		}
	}
	
	@Override
	public List<V> remove(Object key) {
		return innerMap.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends List<V>> m) {
		innerMap.putAll(m);
	}

	@Override
	public void clear() {
		innerMap.clear();
	}

	@Override
	public Set<K> keySet() {
		return innerMap.keySet();
	}

	@Override
	public Collection<List<V>> values() {
		return innerMap.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, List<V>>> entrySet() {
		return innerMap.entrySet();
	}

}
