/*
*    jETeL/Clover.ETL - Java based ETL application framework.
*    Copyright (C) 2002-2004  David Pavlis <david_pavlis@hotmail.com>
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
*/

package org.jetel.data.lookup;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Interface for lookup tables. This is a minimum functionality required.
 * 
 * @author DPavlis
 * @since  8.7.2004
 *
 */
public interface LookupTable {
	
	/**
	 * Called when the lookup table is first used/needed. Usually at
	 * the beginnig of phases in which the lookup is used. Any memory & time intensive
	 * allocation should happen during call to this method.<br>
	 * It may happen that this method is called several times; however it should
	 * be secured that initialization (allocation, etc.) is performed only once or
	 * every time close() method was called prior to this method.<br>
	 * 
	 * @throws JetelException
	 */
	public void init() throws JetelException;
	
	
	/**
	 * Specifies what object type will be used for looking up data.
	 * According to Object type used for calling this method, proper get() method
	 * should be called then.
	 * 
	 * @param key can be one of these Object types - String, Object[], RecordKey
	 */
	public void setLookupKey(Object key);
	
	/**
	 * Return DataRecord stored in lookup table under the specified String
	 * key.
	 * 
	 * @param keyString - the key to be used for looking up DataRecord
	 * @return DataRecord associated with specified key or NULL if not found
	 */
	public DataRecord get(String keyString);

	
	/**
	 * Return DataRecord stored in lookup table under the specified keys.<br>
	 * As a lookup values, only following objects should be used - Integer, Double, Date, String)
	 * 
	 * @param keys values used for look-up 	of data record
	 * @return DataRecord associated with specified keys or NULL if not found
	 * @throws JetelException
	 */
	public DataRecord get(Object[] keys);
	
	/**
	 * Returns DataRecord stored in lookup table. 
	 * 
	 * @param keyRecord DataRecord to be used for looking up data
	 * @return DataRecord associated with specified key or NULL if not found
	 * @throws JetelException
	 */
	public DataRecord get(DataRecord keyRecord);
	
	/**
	 * Next DataRecord stored under the same key as the previous one sucessfully
	 * retrieved while calling get() method.
	 * 
	 * @return DataRecord or NULL if no other DataRecord is stored under the same key
	 */
	public DataRecord getNext();
	
	
	/**
	 * Returns number of DataRecords found by last get() method call.<br>
	 * 
	 * @return number of found data records or -1 if can't be applied for this lookup table
	 * implementation
	 * @throws JetelException
	 */
	public int getNumFound();
	
	/**
	 * Gets the name (String) of the lookup table - used for keeping track of
	 * various lookup tables registered within Transformation graph.
	 * 
	 * @return the name (String) of the lookup table 
	 */
	public String getName();
	
	/**
	 * Gets the metadata associated with the DataRecord stored in the lookup table.
	 * @return the metadata object
	 */
	public DataRecordMetadata getMetadata();
	
	/**
	 * Closes the lookup table - frees any allocated resource (memory, etc.)<br>
	 * This method is called when this lookup table is no more needed during TransformationGraph
	 * execution.<br>
	 * Can be also called from user's transformation method. If close() is called, then
	 * continuation in using lookup table should not be permitted - till init() is called
	 * again.
	 */
	public void close() throws JetelException;
	
}
