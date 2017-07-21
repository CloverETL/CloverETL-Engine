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
package org.jetel.data;

import org.jetel.metadata.DataRecordMetadata;

/**
 * Represents a buffer of two data records that can be swapped.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 29th April 2009
 * @since 29th April 2009
 */
public class DoubleRecordBuffer {

	/** the data records that will hold the data */
	private final DataRecord[] dataRecords = new DataRecord[2];

	/** the index of the current data record */
    private int currentIndex = 0;
	/** the index of the previous data record */
    private int previousIndex = 1;

    /**
     * Constructs an instance of the <code>DoubleRecordBuffer</code> for the given data record metadata.
     *
     * @param metadata metadata of data records that will be stored
     */
    public DoubleRecordBuffer(DataRecordMetadata metadata) {
        for (int i = 0; i < dataRecords.length; i++) {
        	dataRecords[i] = DataRecordFactory.newRecord(metadata);
        	dataRecords[i].init();
        }
	}

    /**
     * @return the current data record
     */
	public DataRecord getCurrent() {
		return dataRecords[currentIndex];
	}

    /**
     * @return the previous data record
     */
	public DataRecord getPrevious() {
		return dataRecords[previousIndex];
	}

	/**
	 * Simply swaps the current and previous data records while not modifying the contents.
	 */
	public void swap() {
        currentIndex ^= 1;
        previousIndex ^= 1;
	}

}
