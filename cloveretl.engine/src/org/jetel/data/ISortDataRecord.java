package org.jetel.data;

import java.io.IOException;
import java.nio.ByteBuffer;

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
	public abstract boolean put(DataRecord record) throws IOException;

	/**
	 *  Sorts internal array and flush it to disk, thus reading can start
	 *  Note: in the case of external sorting, when merge is necessary, other name would be suitable,
	 *  		it is named sort() to maintain consistency with InternalSortDataRecord
	 */
	public abstract void sort() throws IOException, InterruptedException;

	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  recordData  ByteBuffer into which copy next record's data
	 *@return             True if there was next record or False
	 */
	public abstract DataRecord get() throws IOException;

	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  recordData  ByteBuffer into which copy next record's data
	 *@return             True if there was next record or False
	 */
	public abstract boolean get(ByteBuffer recordDataBuffer) throws IOException;

	/**
	 * Frees all resources (buffers, collections of internal sorter, etc) 
	 */
	public abstract void free();

}