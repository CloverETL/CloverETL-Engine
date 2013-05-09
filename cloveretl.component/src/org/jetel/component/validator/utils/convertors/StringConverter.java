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
package org.jetel.component.validator.utils.convertors;

/**
 * Implementation of converter for converting something into string.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
 */
public class StringConverter implements Converter {
	
	private static StringConverter instance;
	private StringConverter() {}
	
	/**
	 * Returns instance of converter
	 * @return Instance of string converter
	 */
	public static StringConverter getInstance() {
		if(instance == null) {
			instance = new StringConverter();
		}
		return instance;
	}

	@Override
	public String convert(Object o) {
		if(o == null) {
			return null;
		} else if(o instanceof byte[]) {
			// On byte arrays take its size into account rather than content
			return "" + ((byte[]) o).length;
		}
		
		return o.toString();
	}
	
	@Override
	public String convertFromCloverLiteral(String o) {
		return o;
	}

}
