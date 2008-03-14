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

import java.util.Properties;

/**
 * Default dictionary entry provider type. It handles string dictionary elements in a "value" attribute.
 * All other attributes are ignored.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 10.3.2008
 */
public class StringDictionaryEntryProvider implements DictionaryEntryProvider {

	private static final String VALUE_ATTRIBUTE = "value";
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.DictionaryEntryProvider#getValue(java.util.Properties)
	 */
	public IDictionaryValue<?> getValue(Properties properties) {
		String value = properties.getProperty(VALUE_ATTRIBUTE);
		
		return new DictionaryValue<String>(value);
	}

}
