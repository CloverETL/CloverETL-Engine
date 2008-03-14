/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
package org.jetel.data.lookup;

import org.jetel.data.DataRecord;

/**
 * "Iterator" over a lookup table.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Aug 13, 2007
 */
public interface LookupTableIterator {
	
	/**
	 * Returns DataRecord stored in lookup table. 
	 * 
	 * @param keyRecord DataRecord to be used for looking up data.
	 * @return DataRecord associated with specified key or <code>null</code> if not found.
	 */
	public DataRecord get(DataRecord keyRecord);
	
	/**
	 * Next DataRecord stored under the same key as the previous one successfully
	 * retrieved while calling get() method.
	 * 
	 * @return DataRecord or <code>null</code> if no other DataRecord is stored under the same key.
	 */
	public DataRecord getNext();
}
