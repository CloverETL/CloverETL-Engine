/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2008  David Pavlis <david.pavlis@javlin.cz>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.data.lookup;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.jetel.data.DataRecord;

/**
 * A basic implementation of a lookup table iterator.
 * <p>
 * This implementation fetches all the data records associated with the given lookup key and
 * stores them in a queue. This approach might not be appropriate for lookup tables that have
 * multiple data records associated with one lookup key.
 *
 * @author Martin Janik <martin.janik@javlin.cz>
 * @since 6th October 2008
 */
public class BasicLookupTableIterator implements Iterator<DataRecord> {

	/** the queue of matching data records returned by this iterator */
	private final Queue<DataRecord> matchingDataRecords = new LinkedList<DataRecord>();

	/**
	 * Constructs a new instance of the <code>BasicLookupTableIterator</code> class.
	 *
	 * @param lookupTable the lookup table for this iterator
	 * @param lookupKey the lookup key used for data record(s) lookup
	 *
	 * @throws <code>NullPointerException</code> if any parameter is null
	 * @throws <code>IllegalArgumentException</code> if the given lookup key is not compatible
	 * with the given lookup table
	 */
	public BasicLookupTableIterator(LookupTable lookupTable, Object lookupKey) {
		if (lookupTable == null) {
			throw new NullPointerException("lookupTable");
		}

		if (lookupKey == null) {
			throw new NullPointerException("lookupKey");
		}

		DataRecord dataRecord = null;

		if (lookupKey instanceof String) {
			dataRecord = lookupTable.get((String) lookupKey);
		} else if (lookupKey instanceof DataRecord) {
			dataRecord = lookupTable.get((DataRecord) lookupKey);
		} else if (lookupKey instanceof Object[]) {
			dataRecord = lookupTable.get((Object[]) lookupKey);
		} else {
			throw new IllegalArgumentException("The lookupKey is not compatible!");
		}

		while (dataRecord != null) {
			this.matchingDataRecords.add(dataRecord);
			dataRecord = lookupTable.getNext();
		}
	}

	public boolean hasNext() {
		return !matchingDataRecords.isEmpty();
	}

	public DataRecord next() {
		return matchingDataRecords.remove();
	}

	public void remove() {
        throw new UnsupportedOperationException("Method not supported!");
	}

}
