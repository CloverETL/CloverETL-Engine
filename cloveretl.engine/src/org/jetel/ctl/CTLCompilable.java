package org.jetel.ctl;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;

public interface CTLCompilable {

	/** 
	 * Method for initialization of global scope
	 */
	public void global() throws ComponentNotReadyException;

	/**
	 * Method to retrieve input record 
	 * @param index
	 * @return data record or null if such port does not exist
	 */
	public DataRecord getInputRecord(int index);
	
	
	/**
	 * Method to retrieve output record
	 * @param index
	 * @return data record or null if such port does not exist
	 */
	public DataRecord getOutputRecord(int index);
	
}
