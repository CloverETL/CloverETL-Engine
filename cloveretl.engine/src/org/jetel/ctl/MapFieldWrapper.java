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
package org.jetel.ctl;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.jetel.data.MapDataField;
import org.jetel.util.MiscUtils;

/**
 * The wrapper converts CharSequences into Strings.
 * 
 * It is used to enable passing maps in input/output records
 * by reference.
 * 
 * It should hide any differences between
 * {@link MapDataField}'s MapDataFieldView
 * and a standard implementation of a Map interface.
 * 
 * Note that this could mean different handling of equals()
 * on DataFields.
 * 
 * @see #entrySet()
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23.1.2012
 */
public class MapFieldWrapper<V> extends AbstractMap<String, V> {
	
	private Map<String, V> parentMap;
	
	private transient volatile Set<Map.Entry<String, V>> entrySet = null;
	
	/**
	 * @param parent
	 */
	@SuppressWarnings("unchecked")
	public MapFieldWrapper(Object parent) {
		if (parent == null) {
			throw new NullPointerException("parent");
		}
		this.parentMap = (Map<String, V>) parent;
	}
	
	@SuppressWarnings("unchecked")
	private static <T> T toCTL(T o) {
		if (o instanceof CharSequence) {
			return (T) o.toString(); // convert CharSequences to Strings
		}
		return o;
	}

	/*
	 * Returns a custom entry set which converts
	 * the values from CloverStrings to Strings.
	 */
	@Override
	public Set<Map.Entry<String, V>> entrySet() {
		if (entrySet == null) {
			entrySet = new EntrySet(parentMap.entrySet()); 
		}
		return entrySet;
	}
	
	/*
	 * Keys are not modified.
	 */
	@Override
	public boolean containsKey(Object key) {
		return parentMap.containsKey(key);
	}

	/*
	 * The return value is converted.
	 */
	@Override
	public V get(Object key) {
		return toCTL(parentMap.get(key));
	}
	
	/*
	 * Overridden just for the sake of performance.
	 * 
	 * The implementation from AbstractCollection would
	 * work correctly. 
	 */
	@Override
	public Set<String> keySet() {
		return parentMap.keySet();
	}

	/*
	 * The return value is converted.
	 */
	@Override
	public V put(String key, V value) {
		return toCTL(parentMap.put(key, value));
	}

	/*
	 * The return value is converted.
	 */
	@Override
	public V remove(Object key) {
		return toCTL(parentMap.remove(key));
	}
	
    /**
     * Converts the return values from CloverStrings to Strings.
     * 
     * @author krivanekm (info@cloveretl.com)
     *         (c) Javlin, a.s. (www.cloveretl.com)
     *
     * @created 31.1.2012
     */
	private class TransformedEntry implements Map.Entry<String, V> {
		
		private Map.Entry<String, V> parentEntry;
		
		public TransformedEntry(Map.Entry<String, V> parentEntry) {
			this.parentEntry = parentEntry;
		}

		@Override
		public String getKey() {
			return parentEntry.getKey();
		}

		/*
		 * Convert the return value.
		 */
		@Override
		public V getValue() {
			return toCTL(parentEntry.getValue());
		}

		/*
		 * Convert the return value.
		 */
		@Override
		public V setValue(V value) {
			return toCTL(parentEntry.setValue(value));
		}

		/**
		 * @see Map.Entry#hashCode()
		 */
		@Override
		public int hashCode() {
			String key = getKey();
			V value = getValue();
		    return (key   == null ? 0 :   key.hashCode()) ^
		 		   (value == null ? 0 : value.hashCode());
		}

		/**
		 * @see Map.Entry#equals(Object)
		 */
	    @Override
		public boolean equals(Object o) {
		    if (!(o instanceof Map.Entry)) {
				return false;
		    }
		    Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
			String key = getKey();
			V value = getValue();
		    return MiscUtils.equals(key, e.getKey()) && MiscUtils.equals(value, e.getValue());
		}

		@Override
		public String toString() {
		    return getKey() + "=" + getValue();
		}
		
	}

	/**
	 * Delegates most of the calls 
	 * to the underlying <code>parentEntrySet</code>.
	 * 
	 * Returns a custom {@link EntrySet#iterator()}.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 31.1.2012
	 */
	private class EntrySet extends AbstractSet<Map.Entry<String, V>> {
		
		private Set<Map.Entry<String, V>> parentEntrySet;
		
		/**
		 * Delegates all calls to <code>parentEntrySet</code>.
		 * 
		 * {@link #next()} wraps the returned Map.Entry
		 * into {@link TransformedEntry}.
		 * 
		 * @author krivanekm (info@cloveretl.com)
		 *         (c) Javlin, a.s. (www.cloveretl.com)
		 *
		 * @created 31.1.2012
		 */
		private class EntrySetIterator implements Iterator<Map.Entry<String, V>> {
			
			private Iterator<Map.Entry<String, V>> parentEntrySetIterator;
			
			public EntrySetIterator(Iterator<Map.Entry<String, V>> parentEntrySetIterator) {
				this.parentEntrySetIterator = parentEntrySetIterator;
			}

			@Override
			public boolean hasNext() {
				return parentEntrySetIterator.hasNext();
			}

			/*
			 * Wrap the returned Map.Entry.
			 */
			@Override
			public Map.Entry<String, V> next() {
				return new TransformedEntry(parentEntrySetIterator.next());
			}

			@Override
			public void remove() {
				parentEntrySetIterator.remove();
			}
			
		}
		
		private EntrySet(Set<Map.Entry<String, V>> parentEntrySet) {
			this.parentEntrySet = parentEntrySet;
		}

		@Override
		public Iterator<Map.Entry<String, V>> iterator() {
			return new EntrySetIterator(parentEntrySet.iterator());
		}

		@Override
		public int size() {
			return parentEntrySet.size();
		}

		@Override
		public void clear() {
			parentEntrySet.clear();
		}
		
		/**
		 * We cannot directly call <code>contains()</code> on
		 * <code>parentEntrySet</code>, because the values
		 * are modified.
		 * 
		 * {@link AbstractCollection#contains(Object)} 
		 * has linear complexity.
		 * 
		 * Because we assume that the underlying map will
		 * be a hash map, we obtain the key from the <code>entry</code>
		 * and then get the translated value by calling 
		 * {@link MapFieldWrapper#get(Object)}.
		 * 
		 * The translated value must be equal to the value
		 * of the <code>entry</code>.
		 */
		private boolean contains(Map.Entry<?, ?> entry) {
            Object key = entry.getKey();
            
            if (!MapFieldWrapper.this.containsKey(key)) {
            	return false;
            }
            
            V value = MapFieldWrapper.this.get(key);
            return MiscUtils.equals(entry.getValue(), value);
		}

		/**
		 * @see #contains(java.util.Map.Entry)
		 */
		@Override
		public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<?,?> e = (Map.Entry<?,?>) o;
            return contains(e);
		}

		/**
		 * @see #contains(java.util.Map.Entry)
		 * 
		 * We cannot directly call <code>remove()</code> on
		 * <code>parentEntrySet</code>.
		 * 
		 * {@link AbstractCollection#remove(Object)} has
		 * linear complexity.
		 * 
		 * We assume that the underlying map is a hash map,
		 * therefore we obtain the key from the map entry
		 * and remove the entry by calling 
		 * {@link MapFieldWrapper#remove(Object)}.
		 */
		@Override
		public boolean remove(Object o) {
	        if (!(o instanceof Map.Entry)) {
	            return false;
	        }

	        Map.Entry<?,?> entry = (Map.Entry<?,?>) o;
	        if (!contains(entry)) {
	        	return false;
	        }
	        
			return MapFieldWrapper.this.remove(entry.getKey()) != null;
		}

	}
	
}
