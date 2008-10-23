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
package org.jetel.lookup;

import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.lookup.LookupTable;
import org.jetel.graph.GraphElement;

/**
 * Represents an abstract lookup table. This class may be used as a parent class when creating new implementations
 * of lookup tables. Methods common to all the lookup tables should be placed here.
 *
 * @author Martin Janik <martin.janik@javlin.cz>
 *
 * @version 23rd October 2008
 * @since 23rd October 2008
 */
public abstract class AbstractLookupTable extends GraphElement implements LookupTable {

    /**
     * Constructs a new instance of the <code>AbstractLookupTable</code> class.
     *
     * @param id the id of the lookup table within the graph
     */
    public AbstractLookupTable(String id) {
        super(id);
    }

    public boolean isReadOnly() {
        return true;
    }

    public boolean put(DataRecord dataRecord) {
        throw new UnsupportedOperationException("Method not supported!");
    }

    public boolean remove(DataRecord dataRecord) {
        throw new UnsupportedOperationException("Method not supported!");
    }

    public int get(HashKey lookupKey, List<DataRecord> matchingDataRecords) {
        if (lookupKey == null) {
            throw new NullPointerException("lookupKey");
        }

        if (matchingDataRecords == null) {
            throw new NullPointerException("matchingDataRecords");
        }

        int matchingDataRecordsCount = 0;
        DataRecord dataRecord = get(lookupKey);

        while (dataRecord != null) {
            matchingDataRecords.add(dataRecord);
            matchingDataRecordsCount++;

            dataRecord = getNext();
        }

        return matchingDataRecordsCount;
    }

    @Override
    public String toString() {
        return (getClass().getSimpleName() + " (id = " + getId() + ", name = " + getName() + ")");
    }

}
