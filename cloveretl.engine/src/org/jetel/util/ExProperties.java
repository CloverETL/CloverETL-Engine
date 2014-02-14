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
package org.jetel.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an alternative to standard properties
 * 
 * It allows a property to have more than one value.
 * 
 * @author Pavel
 * 
 */
public class ExProperties extends LinkedHashMap<String, ArrayList<String>> {

	/**
	 * Default is '|' character which is ignored when escaped (i.e. '\|')
	 */
	final static String VALUE_SEPARATOR_REGEX = "(?<!\\\\)\\|"; //$NON-NLS-1$

	final static String KEY_SEPARATOR="="; //$NON-NLS-1$
	final static String PROPERTY_SEPARATOR="\n"; //$NON-NLS-1$

	final static String KEY_SEPARATOR_ENC="&#61;"; //$NON-NLS-1$

	/**
	 * Returns the implicit (first) value for `key`
	 * 
	 * To get all values for a key, refer to getValues(String)
	 * 
	 * @param key
	 * @return
	 */
	public String get(String key) {
		return get(key, 0);
	}

	/**
	 * Return a value for property `key` at index `index`
	 * 
	 * If the value does not exist, returns null (no exception)
	 * 
	 * @param key
	 * @param index
	 * @return
	 */
	public String get(String key, int index) {
		if (index < 0) {
			return null;
		}

		ArrayList<String> value = super.get(key);

		if (value == null || (index > value.size() - 1)) {
			return null;
		}

		return value.get(index);

	}

	/**
	 * Return all values for `key`
	 * 
	 * @param key
	 * @return
	 */
	public ArrayList<String> getValues(String key) {
		return super.get(key);
	}

	/**
	 * Sets value for `key` at index `index`
	 * 
	 * @param key
	 * @param value
	 * @param index
	 */
	public void put(String key, String value, int index) {
		if (value == null) {
			return;
		}

		ArrayList<String> values = super.get(key);

		if (values == null) {
			values = new ArrayList<String>();
			super.put(key, values);
		}

		if (values.size() <= index) {
			int start = values.size();
			for (int i = start; i < index; i++) {
				values.add(null);
			}
			values.add(value);
		} else {
			values.set(index, value);
		}

	}

	public void put(String key, String value) {
		put(key, value, 0);
	}

	public static String[] parseAlternatives(String s) {
		return s != null ? s.split(VALUE_SEPARATOR_REGEX) : null;
	}
	
	public void fromString(String value) throws IOException{
		String propName="", propValue=""; //$NON-NLS-1$ //$NON-NLS-2$
		String currentLine, prevLine=""; //$NON-NLS-1$
		boolean multiline = false;
		
		BufferedReader reader = new BufferedReader(new StringReader(value));
		while ((currentLine = reader.readLine()) != null) {
			if (currentLine.indexOf(KEY_SEPARATOR) < 0) { // current line does not contain =
				// append value
				propValue += "\n" + currentLine; //$NON-NLS-1$
				multiline = true;
			} else {  // current line contains =
				// save last property
                if (prevLine!=null && (multiline || prevLine.indexOf(KEY_SEPARATOR) >= 0)) {
                    putProperty(propName, propValue);
                    multiline = false;
                } 
                // load new property
                String[] parts = currentLine.split(KEY_SEPARATOR, 2);
                propName = parts[0];
                propValue = parts.length > 1 ? parts[1] : ""; //$NON-NLS-1$
			}
			prevLine = currentLine;
		}
        putProperty(propName, propValue);
	}
	
	private void putProperty(String propName, String propValue) {
		if (propName == null || propName.isEmpty()) {
			return;
		}
        String[] parts = ExProperties.parseAlternatives(propValue);
        
        ArrayList<String> values = getValues(propName);
        if (values == null) {
            values = new ArrayList<String>();
            super.put(propName, values);
        }

        if (parts != null) {
            for (String part : parts) {
                values.add(deEscape(part));
            }
        }
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, ArrayList<String>> entry : entrySet()) {
			sb.append(entry.getKey());
			sb.append(KEY_SEPARATOR);
			sb.append(getValuesAsString(entry.getKey()));
			sb.append(PROPERTY_SEPARATOR);
		}
		return sb.toString();
	}

	public String getValuesAsString(String key) {
		return getValuesAsString(key, 0, -1);
	}
	

	public String getValuesAsString(String key, int startIndex, int endIndex) {
		return getValuesAsString(key, startIndex, endIndex, true);
	}

	/**
	 * Returns values of a key in serialized (String) form
	 * @param startIndex First value to serialize (till end)
	 * @return
	 */
	public String getValuesAsString(String key, int startIndex, int endIndex, boolean escape) {
		StringBuilder sb = new StringBuilder();
		ArrayList<String> values = getValues(key);
		boolean atLeastSome = false;
		if (endIndex < 0 || endIndex >= values.size()) {
			endIndex = values.size()-1;
		}
		if (startIndex < 0) {
			startIndex = 0;
		}
		for (int i = startIndex; i <= endIndex; i++) {
			atLeastSome = true;
			if (i > startIndex) {
				sb.append("|");
			}
			String value = values.get(i);
			if (value != null) {
                sb.append(escape ? escape(value) : value);
			}
		}
		return atLeastSome ? sb.toString() : null;
	}
	
	static String escape(String s) {
		return s == null ? null : s.replaceAll("\\|", "\\\\|").replaceAll("\r\n", "\\\\r\\\\n").replaceAll(KEY_SEPARATOR, KEY_SEPARATOR_ENC);
	}

	static String deEscape(String s) {
		return s == null ? null : s.replaceAll("\\\\\\|", "|").replaceAll("\\\\r\\\\n", "\r\n").replaceAll(KEY_SEPARATOR_ENC, KEY_SEPARATOR);
	}
	
}
