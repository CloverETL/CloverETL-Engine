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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jetel.util.bytes.CloverBuffer;

/**
 * 
 * Common interface for sorting - 
 * 
 * @author Jakub Lehotsky (jakub.lehotsky@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Jan 16, 2008
 */
public interface ISortDataRecord {

	/**
	 *  Stores additional record into internal buffer for sorting. Implementors are sorters,
	 *  which can be based on various algorithm. According to this interface, they must hold
	 *  to this pattern:
	 *  
	 *  User calls:
	 *  get() n-times
	 *  sort()
	 *  put() n-times
	 *  
	 *  User receives DataRecord items one by one by calling put() in sorted order.  
	 *  
	 *
	 *@param  record  DataRecord to be stored
	 */
	public boolean put(DataRecord record) throws IOException, InterruptedException;

	/**
	 *  Sorts internal array and flush it to disk, thus reading can start
	 *  Note: in the case of external sorting, when merge is necessary, other name would be suitable,
	 *  		it is named sort() to maintain consistency with InternalSortDataRecord
	 */
	public void sort() throws IOException, InterruptedException;

	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  recordData  ByteBuffer into which copy next record's data
	 *@return             True if there was next record or False
	 */
	public DataRecord get() throws IOException, InterruptedException;

	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  recordData  ByteBuffer into which copy next record's data
	 *@return             True if there was next record or False
	 */
	public boolean get(CloverBuffer recordDataBuffer) throws IOException, InterruptedException;

	/**
	 * @deprecated use {@link #get(CloverBuffer)} instead
	 */
	@Deprecated
	public boolean get(ByteBuffer recordDataBuffer) throws IOException, InterruptedException;

	/**
	 * Resets all resources (buffers, collections of internal sorter, etc), so component can
	 * be used in next run again
	 */
	public void reset();
	
	/**
	 * Frees all resources required for previous single run of component. 
	 */
	public void postExecute();
	
	/**
	 * Frees all resources (buffers, collections of internal sorter, etc) 
	 */
	public void free() throws InterruptedException;

}