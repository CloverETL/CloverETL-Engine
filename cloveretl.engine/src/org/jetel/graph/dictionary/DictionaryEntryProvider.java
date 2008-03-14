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

import org.jetel.exception.AttributeNotFoundException;

/**
 * This interface serves to provide ability to build up dictionary value from the given properties.
 * It is intended to be implemented in various external plugins for their internal dictionary value types.
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 10.3.2008
 */
public interface DictionaryEntryProvider {

	public static final String DEFAULT_TYPE = "string";

	/**
	 * Returns a dictionary value based on the given properties.
	 * @param properties
	 * @return
	 */
	public IDictionaryValue<?> getValue(Properties properties) throws AttributeNotFoundException;
	
}
