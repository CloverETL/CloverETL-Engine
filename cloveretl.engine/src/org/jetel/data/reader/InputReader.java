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
public interface InputReader {
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
	public int compare(InputReader other);
	
	public boolean hasData();
}