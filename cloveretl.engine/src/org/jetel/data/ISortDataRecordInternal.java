package org.jetel.data;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ISortDataRecordInternal {

	/**
	 *  Stores additional record into internal buffer for sorting
	 *
	 *@param  record  DataRecord to be stored
	 */
	public abstract boolean put(DataRecord record) throws IOException;

	/**
	 *  Sorts internal array and flush it to disk, thus reading can start
	 *  Note: in the case of external sorting, when merge is necessary, other name would be suitable,
	 *  		it is named sort() to maintain consistency with SortDataRecordInternal
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