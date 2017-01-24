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
package org.jetel.graph.dictionary;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.string.StringUtils;

/**
 * Clover dictionary representation. That is just simple wrapper around Map<String, Object> class.
 * It is only data container without data type validation.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 4.2.2010
 */
public final class DictionaryValuesContainer implements Serializable {
	private static final long serialVersionUID = 3190840308230414228L;
	private static Logger log = Logger.getLogger(DictionaryValuesContainer.class);
	
	private final Map<String, Serializable> values;
	private final Set<String> dirtyKeys;
	private Set<String> nonPersistableKeys;

	/**
	 * 
	 * @author mvarecha (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 16, 2012
	 */
	public static class InputStreamSerializableHandler implements Serializable {
		private static final long serialVersionUID = 2437492766546045126L;
		transient private final ReadableByteChannel channel;

		public InputStreamSerializableHandler(ReadableByteChannel channel) {
			super();
			if (channel == null)
				throw new IllegalArgumentException("ReadableByteChannel must be specified");
			this.channel = channel;
		}

		public ReadableByteChannel getChannel() {
			if (channel == null) {
				throw new IllegalStateException("Deserialized without the ReadableByteChannel!");
			}
			return channel;
		}
	}// class
	
	/**
	 * 
	 * @author mvarecha (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 16, 2012
	 */
	public static class OutputStreamSerializableHandler implements Serializable {
		private static final long serialVersionUID = -1270971860260897043L;
		transient private final WritableByteChannel channel;

		public OutputStreamSerializableHandler(WritableByteChannel channel) {
			super();
			if (channel == null)
				throw new IllegalArgumentException("WritableByteChannel must be specified");
			this.channel = channel;
		}

		public WritableByteChannel getChannel() {
			if (channel == null) {
				throw new IllegalStateException("Deserialized without the WritableByteChannel!");
			}
			return channel;
		}
	}// class
	
	/**
	 * 
	 * @param dictionary
	 * @return
	 */
	public static DictionaryValuesContainer getInstance(Dictionary dictionary) {
		return getDictionaryValuesContainer(dictionary, true, true, false);
	}

	public static DictionaryValuesContainer getDictionaryValuesContainer(Dictionary dictionary, boolean includeInput, boolean includeOutput) {
		return getDictionaryValuesContainer(dictionary, includeInput, includeOutput, false);
	}
	
	/**
	 * Creates DictionaryValuesContainer from specified Dictionary.
	 * It's expected that all values are serializable. 
	 * TODO handle streams
	 * 
	 * @throws UnsupportedOperationException when the dictionary contains output stream.
	 * @param engineDictionary
	 * @param includeInputs - dict entries with input=true will be included
	 * @param includeOutputs - dict entries with output=true will be included
	 * @param includeNonDefined - dict entries with input=false and output=false will be included (those which are not specified in the graph, but the value is somehow set)
	 * @return
	 */
	public static DictionaryValuesContainer getDictionaryValuesContainer(Dictionary dictionary, boolean includeInput, boolean includeOutput, boolean includeNonDefined) {
		DictionaryValuesContainer result = new DictionaryValuesContainer();

		if (dictionary != null) {
			for (String entryName : dictionary.getKeys()) {
				final DictionaryEntry entry = dictionary.getEntry(entryName);
				Object val = entry.getValue();
				//we are interested in output dictionary entries
				if ((entry.isInput() && includeInput) 
						|| (entry.isOutput() && includeOutput) 
						|| (includeNonDefined && !entry.isInput() && !entry.isOutput())) {
					if (isSerializable(val)) {
						result.values.put(entryName, (Serializable)val);
						if (entry.isDirty()) {
							result.dirtyKeys.add(entryName);
						}
					} else {
						log.warn("Non-serializable dictionary entry: key: "+entryName+", value: "+val);
						result.nonPersistableKeys.add(entryName);
					}
				}
			}
		}
		return result;
	}
	
	public static DictionaryValuesContainer duplicate(DictionaryValuesContainer dictionaryContainer) {
		if (dictionaryContainer != null) {
			return dictionaryContainer.duplicate();
		} else {
			return null;
		}
	}

	/**
	 * Goes through specified DictionaryValuesContainer and modified values puts to the specified Dictionary.
	 * Thus modifications made by another worker in the previous phase may be applied to this worker before next phase)
	 * We don't expect any conflicts, since the dictionary is already merged.
	 * 
	 * @param dictionary - target dictionary
	 * @param mergedDictionaryContainer - source dictionary merged from all workers
	 * @throws ComponentNotReadyException 
	 */
	public static void setModifiedValues(Dictionary dictionary, DictionaryValuesContainer mergedDictionaryContainer) throws ComponentNotReadyException {
		if (mergedDictionaryContainer == null) {
			return;
		}
		synchronized (mergedDictionaryContainer.values) {
			for (String key : mergedDictionaryContainer.values.keySet()) {
				if (mergedDictionaryContainer.isDirty(key)) {
					dictionary.setValue(key, mergedDictionaryContainer.values.get(key));
					dictionary.resetDirty(key); // dictionary is synchronized with another workers, so it's not dirty any more
				}
			}
		}
	}
	
	public static boolean isSerializable(Object object) {
		
		if (object == null) {
			return true;
		}
		if (!(object instanceof Serializable)) {
			return false;
		}
		/*
		 * CLO-10089 - the object has to be not only serializable,
		 * but also its type has to be accessible on the core level
		 */
		ClassLoader loader = DictionaryValuesContainer.class.getClassLoader();
		try {
			Class<?> type = loader.loadClass(object.getClass().getName());
			if (!type.equals(object.getClass())) {
				return false;
			}
		} catch (Exception e) {
			return false;
		}
		
		if (object instanceof Collection<?>) {
			Collection<?> col = (Collection<?>)object;
			for (Object item : col) {
				if (!isSerializable(item)) {
					return false;
				}
			}
		} else if (object instanceof Map<?, ?>) {
			Map<?, ?> map = (Map<?, ?>)object;
			for (Entry<?, ?> e : map.entrySet()) {
				if (!isSerializable(e.getKey()) || !isSerializable(e.getValue())) {
					return false;
				}
			}
		}
		// TODO arrays?
		
		return true;
	}
	
	/**
	 * 
	 */
	public DictionaryValuesContainer() {
		values = new HashMap<String, Serializable>();
		dirtyKeys = new HashSet<String>();
		nonPersistableKeys = new HashSet<>();
	}

	public void setValue(String key, Serializable value) {
		if (key == null) {
			throw new IllegalArgumentException("Dictionary key cannot be null.");
		}
		synchronized (values) {
			values.put(key, value);
			dirtyKeys.add(key);
		}
	}

	public void setValue(String key, InputStream is) {
		Serializable value = new InputStreamSerializableHandler(Channels.newChannel(is));
		setValue(key, value);
	}
	public void setValue(String key, ReadableByteChannel channel) {
		Serializable value = new InputStreamSerializableHandler(channel);
		setValue(key, value);
	}
	public void setValue(String key, OutputStream os) {
		Serializable value = new OutputStreamSerializableHandler(Channels.newChannel(os));
		setValue(key, value);
	}
	public void setValue(String key, WritableByteChannel channel) {
		Serializable value = new OutputStreamSerializableHandler(channel);
		setValue(key, value);
	}

	public void setValue(String key, Object o) {
		if (o == null)
			setValue(key, (Serializable)o);
		else if (o instanceof Serializable)
			setValue(key, (Serializable)o);
		else if (o instanceof InputStream)
			setValue(key, (InputStream)o);
		else if (o instanceof OutputStream)
			setValue(key, (OutputStream)o);
		else if (o instanceof ReadableByteChannel)
			setValue(key, (ReadableByteChannel)o);
		else if (o instanceof WritableByteChannel)
			setValue(key, (WritableByteChannel)o);
		else 
			throw new IllegalArgumentException("DictionaryContainer accepts only Serializable objects and I/O streams!");
	}
	
	/*
	public Map<String, Serializable> getValues() {
		return values;
	}*/

	public boolean isEmpty() {
		synchronized (values) {
			return values.isEmpty(); 
		}
	}
	
	public void clear() {
		synchronized (values) {
			values.clear();
			dirtyKeys.clear();
			nonPersistableKeys.clear();
		}
	}
	
	public Set<String> getKeys() {
		synchronized (values) {
			return new HashSet<String>(values.keySet());
		}
	}

	/**
	 * Returns value of specified key. It's instance of Serializable or Channel.
	 * @param key
	 * @return
	 */
	public Object getValue(String key) {
		Serializable s = null;
		synchronized (values) {
			s = values.get(key);
		}
		if (s instanceof InputStreamSerializableHandler) {
			return ((InputStreamSerializableHandler)s).getChannel();
		} else if (s instanceof OutputStreamSerializableHandler) {
			return ((OutputStreamSerializableHandler)s).getChannel();
		}
		return s;
	}

	public boolean isDirty(String key) {
		synchronized (values) {
			return dirtyKeys.contains(key);
		}
	}

	/**
	 * Returns shallow copy of the dictionary content.
	 * TreeMap sorts by the keys.
	 * @return
	 */
	public SortedMap<String, Serializable> getContent() {
		SortedMap<String, Serializable> tm = new TreeMap<String, Serializable>();
		synchronized (values) {
			tm.putAll(values);
		}
		return tm;
	}
	
	public boolean hasNonPersistableKeys() {
		synchronized (values) {
			return !nonPersistableKeys.isEmpty();
		}
	}
	
	/**
	 * Creates shallow copy. There will be references from the original and from the copy to the same instance. 
	 * @return
	 */
	public DictionaryValuesContainer duplicate() {
		DictionaryValuesContainer result = new DictionaryValuesContainer();
		synchronized (values) {
			result.values.putAll(values);
			result.dirtyKeys.addAll(dirtyKeys);
			result.nonPersistableKeys.addAll(nonPersistableKeys);
		}
		return result;
	}
	
	public Properties toProperties() {
		Properties result = new Properties();
		synchronized (values) {
			for (Entry<String, Serializable> entry : values.entrySet()) {
				result.setProperty(entry.getKey(), StringUtils.specCharToString(String.valueOf(entry.getValue())));
			}
		}
		return result;
	}
	
	@Override
	public String toString() {
		return toString(true);
	}
	
	public String toString(final boolean singleLine) {
		
		StringBuilder sb = new StringBuilder();
		if (singleLine) {
			sb.append("{ ");
		}
		synchronized (values) {
			List<String> entries = new ArrayList<>(values.keySet());
			Collections.sort(entries);
			for (Iterator<String> it = entries.iterator(); it.hasNext();) {
				String name = it.next();
				if (isDirty(name)) {
					sb.append('*');
				}
				sb.append(name).append('=').append(String.valueOf(values.get(name)));
				if (it.hasNext()) {
					if (singleLine) {
						sb.append(", ");
					} else {
						sb.append('\n');
					}
				}
			}
		}
		if (singleLine) {
			sb.append(" }");
		}
		return sb.toString();
	}
	
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
	}
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject(); 
        if (nonPersistableKeys == null) {
        	nonPersistableKeys = new HashSet<>();
        }
	}

	/**
	 * Removes entries from specified dicContainer according to definition in the Dictionary.
	 * @param dictionaryContainer
	 * @param dictionaryDefinition
	 * @param keepInput
	 * @param keepOutput
	 * @param keepNonDefined
	 * @return
	 */
	public static DictionaryValuesContainer filterDictionaryEntries(DictionaryValuesContainer dictionaryContainer,	Dictionary dictionaryDefinition, boolean keepInput, boolean keepOutput, boolean keepNonDefined) {
		DictionaryValuesContainer result = dictionaryContainer.duplicate();
		synchronized (dictionaryContainer.values) {
			for (String key : dictionaryContainer.values.keySet()) {
				final DictionaryEntry entry = dictionaryDefinition.getEntry(key);
				if ((entry.isInput() && keepInput) 
						|| (entry.isOutput() && keepOutput) 
						|| (keepNonDefined && !entry.isInput() && !entry.isOutput())) {
					// we keep the entry
				} else {
					result.remove(key);
				}

			}// for
		}// sync
		return result;
	}

	private void remove(String key) {
		synchronized (values) {
			values.remove(key); 
		}
	}
	
}
