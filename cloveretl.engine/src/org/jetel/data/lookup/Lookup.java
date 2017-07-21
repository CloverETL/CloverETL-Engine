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
package org.jetel.data.lookup;

import java.util.Iterator;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;

/**
 * This interface serves as a provider to lookup table data. Whenever user wants to access to lookup table data
 * associated with a key, he has to create this 'proxy' object via createLookup() method invoked on a lookup table.
 * Initialization of lookup is performed during the first calling of {@link #seek()} or {@link #seek(DataRecord)} method. 
 * Then the given instance provides all appropriate records through the Iterator<DataRecord> interface.
 * The owner of this object can whenever change query to the lookup table and consequently call seek() method
 * to restart iterator on a different location of lookup table data. 
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 29.10.2008
 */
public interface Lookup extends Iterator<DataRecord> {

	/**
	 * @return underlying lookup table
	 */
	public LookupTable getLookupTable();

	/**
	 * @return record key used for performing lookup on underlying lookup table
	 */
	public RecordKey getKey();

	/**
	 * Performs lookup based on data stored in the inner data record.
	 *
     * @throws IllegalStateException if the inner data record has not yet been initialized
     *
	 * @see
	 * {@link LookupTable#createLookup(RecordKey, DataRecord)} <br>
	 * {@link #seek(DataRecord)}
	 */
	public void seek();

	/**
	 * Performs lookup based on data in the given record.
	 *
	 * @param keyRecord data record for performing lookup
	 *
     * @throws NullPointerException if the given lookup key record is <code>null</code>
	 */
	public void seek(DataRecord keyRecord);

	/**
	 * Returns size of the underlying collection. <br>
	 * It is not recommended to use this method as in some implementations it simply iterates this iterator.
	 *
	 * @return number of records found by last {@link #seek()} or {@link #seek(DataRecord)} method (current size of 
	 * this underlaying collection) or -1 if the number can't be determined.
	 *
     * @throws IllegalStateException if no seeking has not yet been performed
	 */
	public int getNumFound();

}
