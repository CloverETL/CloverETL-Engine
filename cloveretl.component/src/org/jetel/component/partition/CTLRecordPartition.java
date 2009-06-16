/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 24.4.2005
 *
 */
package org.jetel.component.partition;

import org.jetel.ctl.CTLCompilable;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;

/**
 * Simple interface for partition functions.
 * 
 * @author michal.tomcanyi@javlin.cz
 * @since 2.4.2009
 */

public abstract class CTLRecordPartition implements PartitionFunction, CTLCompilable {

	private DataRecord inputRecord;
	/**
	 * @param record
	 *            data record which should be used for determining partition�
	 *            number
	 * @return port number which should be used for sending data out.
	 */
	@CTLEntryPoint(
			name = "getOutputPort", 
			required = true
	)
	public abstract int getOutputPortDelegate() throws ComponentNotReadyException, TransformException;
	
	/**
	 * Implementation of interface method {@link PartitionFunction#getOutputPort(DataRecord)}.
	 * Delegates the call to CTL universal method {@link #getOutputPort(DataRecord[])}
	 */
	public int getOutputPort(DataRecord record) throws TransformException {
		inputRecord = record;
		try {
			return getOutputPortDelegate();
		} catch (ComponentNotReadyException e) {
			// the exception may be thrown by lookups etc...
			throw new TransformException("Generated transformation class threw an exception",e);
		}
	}

	/**
	 * Called before partition function is first used (getOutputPort is used).
	 * 
	 * @param numPartitions
	 *            how many partitions we have
	 * @param recordKey
	 *            set of fields composing key based on which should the
	 *            partition be determined
	 */

	public void init(int numPartitions, RecordKey partitionKey)	throws ComponentNotReadyException {
		// initialize global scope
		global();
		// call user-defined initialization
		init(numPartitions);
	}

	
	@CTLEntryPoint(
			required = false,
			name = "init",
			parameterNames = { "partitionCount" }
	)
	public void init(Integer partitionCount) throws ComponentNotReadyException {
		// does nothing
	}
	
	/**
	 * Method for CTL compilation. Contains code from global scope and global
	 * variables initialization.
	 * 
	 * @param in
	 *            input records
	 * @param out
	 *            output records
	 */
	@CTLEntryPoint(
			required = false, 
			name = "global" 
	)
	public void global() throws ComponentNotReadyException {
		// does nothing
	}
	
	public DataRecord getInputRecord(int index) {
		return index == 0 ? inputRecord : null;
	}
	
	public DataRecord getOutputRecord(int index) {
		return null;
	}
}
