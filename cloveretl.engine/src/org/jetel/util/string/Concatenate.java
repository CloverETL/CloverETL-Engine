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
package org.jetel.util.string;

import java.util.ArrayList;
import java.util.List;


/**
 * Class for concatenating string using a glue (separator)
 * 
 * Can be used either as state machine or by calling a single method.
 * 
 * State machine:
 * Concatenate c = new Concatenate(";");
 * c.append("stringA");
 * c.append("stringB;");
 * c.append("stringC");
 * String return = c.toString();
 * // c == "stringA;stringB;stringC";
 * 
 * Single method:
 * String return = Concatenate.toString(";", new String[] { "stringA", "stringB;", "stringC" });
 * 
 * @author pnajvar
 *
 */
public class Concatenate {

	public static final String DEFAULT_GLUE = ";";
	
	String glue = DEFAULT_GLUE;
	List<String> items = new ArrayList<String>();

	public Concatenate() {
	}
	
	public Concatenate(String glue) {
		setSeparator(glue);
	}

	public boolean isEmpty() {
		return items.isEmpty();
	}

	public void append(String s) {
		items.add(s);
	}

	public void clear() {
		items.clear();
	}
	
	/**
	 * Returns concatenated items
	 * 
	 * @return All appended items glued together
	 */
	@Override
	public String toString() {
		boolean first = true;
		StringBuilder sb = new StringBuilder();
		for(String item : this.items) {
			if (StringUtils.isEmpty(item)) {
				// ignore empty values
				continue;
			}
			
			if (! first) {
				sb.append(glue);
			}
			if (item.endsWith(glue)) {
				sb.append(item.substring(0, item.length() - glue.length()));
			} else {
				sb.append(item);
			}
			first = false;
		}
		return sb.length() == 0 ? null : sb.toString();
	}
	
	/**
	 * Concatenates `strings` using default separator
	 * 
	 * @param strings Strings to concatenate
	 * @return Strings concatenated into one string using default glue
	 */
	public static String toString(String[] strings) {
		Concatenate c = new Concatenate();
		for(String s : strings) {
			c.append(s);
		}
		return c.toString();
	}
	
	/**
	 * Concatenates `strings` using custom glue
	 * 
	 * @param glue A custom glue 
	 * @param strings Strings to concatenate
	 * @return Strings concatenated using custom glue
	 */
	public static String toString(String glue, String[] strings) {
		Concatenate c = new Concatenate(glue);
		for(String s : strings) {
			c.append(s);
		}
		return c.toString();
	}
	
	/**
	 * Concatenates `strings` using default separator
	 * 
	 * @param strings Strings to concatenate
	 * @return Strings concatenated into one string using default glue
	 */
	public static String toString(List<String> strings) {
		Concatenate c = new Concatenate();
		for(String s : strings) {
			c.append(s);
		}
		return c.toString();
	}
	
	/**
	 * Concatenates `strings` using custom glue
	 * 
	 * @param glue A custom glue 
	 * @param strings Strings to concatenate
	 * @return Strings concatenated using custom glue
	 */
	public static String toString(String glue, List<String> strings) {
		Concatenate c = new Concatenate(glue);
		for(String s : strings) {
			c.append(s);
		}
		return c.toString();
	}
	
	
	
	
	public String getSeparator() {
		return glue;
	}

	public void setSeparator(String separator) {
		this.glue = separator;
	}



}
