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
import java.io.NotSerializableException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
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
		DictionaryValuesContainer result = new DictionaryValuesContainer();

		if (dictionary != null) {
			for (String entryName : dictionary.getKeys()) {
				final DictionaryEntry entry = dictionary.getEntry(entryName);
				Object val = entry.getValue();
				if (val instanceof Serializable) {
					result.setValue(entryName, (Serializable)val);
				} else {
					log.warn("Non-Serializable Dictionary entry: key:"+entryName+" value:"+val);
				}
			}
		}
		
		return result;
	}

	public static DictionaryValuesContainer duplicate(DictionaryValuesContainer dictionaryContainer) {
		DictionaryValuesContainer result = new DictionaryValuesContainer();

		if (dictionaryContainer != null) {
			synchronized (dictionaryContainer.values) {
				result.values.putAll(dictionaryContainer.values);
			}
		}
		
		return result;
	}

	/**
	 * 
	 */
	public DictionaryValuesContainer() {
		values = new HashMap<String, Serializable>();
	}
	
	public void setValue(String key, Serializable value) {
		if (key == null) {
			throw new IllegalArgumentException("Dictionary key cannot be null.");
		}
		synchronized (values) {
			values.put(key, value);
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

	/**
	 * Returns shallow copy of the dictionary content.
	 * TreeMap sorts by the keys.
	 * @return
	 */
	public TreeMap<String, Serializable> getContent() {
		TreeMap<String, Serializable> tm = new TreeMap<String, Serializable>();
		synchronized (values) {
			tm.putAll(values);
		}
		return tm;
	}
	
	/**
	 * Creates shallow copy. There will be references from the original and from the copy to the same instance. 
	 * @return
	 */
	public DictionaryValuesContainer duplicate() {
		DictionaryValuesContainer result = new DictionaryValuesContainer();
		synchronized (values) {
			result.values.putAll(values);
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
		StringBuilder result = new StringBuilder("{ ");
		boolean first = true;
		synchronized (values) {
			for (Entry<String, Serializable> entry : values.entrySet()) {
				if (!first) {
					result.append(", ");
				} else {
					first = false;
				}
				result.append(entry.getKey()).append("=").append(String.valueOf(entry.getValue()));
			}
		}
		result.append(" }");
		return result.toString();
	}
	
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		try{  
	        out.defaultWriteObject();  
	    } catch(NotSerializableException nse){ 
	    	log.error("Can't serialize dictionary content. Serialized data is incomplete! ", nse);
	    }  
	}
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();  
	}
	
}
