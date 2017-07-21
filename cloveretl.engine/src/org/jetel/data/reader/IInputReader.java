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
package org.jetel.data.reader;

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;

/**
 * Interface specifying operations for reading ordered record input.
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public interface IInputReader {
	
	public enum InputOrdering {
		UNDEFINED,
		ASCENDING, 
		DESCENDING,
		UNSORTED
	}
	
	/**
	 * Returns recognized ordering of input data. It may change during data processing.
	 * i.e. at first it's UNDEFINED, 
	 * when greater value is loaded it's ASCENDING and 
	 * if some of next value is lower it may change to UNSORTED.
	 * @return
	 */
	public InputOrdering getOrdering();
	
	/**
	 * Loads next run (set of records with identical keys)  
	 * @return
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public boolean loadNextRun() throws InterruptedException, IOException;

	public void free();

	public void reset() throws ComponentNotReadyException;

	/**
	 * Retrieves one record from current run. Modifies internal data so that next call
	 * of this operation will return following record. 
	 * @return null on end of run, retrieved record otherwise
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public DataRecord next() throws IOException, InterruptedException;

	/**
	 * Resets current run so that it can be read again.  
	 */
	public void rewindRun();
	
	/**
	 * Retrieves one record from current run. Doesn't affect results of sebsequent next() operations.
	 * @return
	 */
	public DataRecord getSample();

	/**
	 * Returns key used to compare data records. 
	 * @return
	 */
	public RecordKey getKey();
	
	/**
	 * Compares reader with another one. The comparison is based on key values of record in the current run
	 * @param other
	 * @return
	 */
	public int compare(IInputReader other);
	
	public boolean hasData();
	
	/**
	 * Return String with info about number of current record, and value of current and previous record
	 * @return
	 */
	public String getInfo();
	
}