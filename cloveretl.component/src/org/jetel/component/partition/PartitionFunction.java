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
package org.jetel.component.partition;

import java.nio.ByteBuffer;

import org.jetel.component.Transform;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;

/**
 * Simple interface for partition functions.
 *
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th June 2010
 * @created 1st March 2005
 *
 * @see org.jetel.component.Partition
 */
public interface PartitionFunction extends Transform {

	/**
	 * Indicates whether partition function supports operation on serialized records /aka direct
	 * 
	 * @return true if getOutputPort(ByteBuffer) method can be called
	 */
	public boolean supportsDirectRecord();

	/**
	 * Called befor partiton function is first used (getOutputPort is used).
	 * 
	 * @param numPartitions how many partitions we have
	 * @param recordKey set of fields composing key based on which should the partition be determined
	 */
	public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException;

	/**
	 * @param record data record which should be used for determining partition??? number
	 * @return port number which should be used for sending data out.
	 * @throws TransformException
	 */
	public int getOutputPort(DataRecord record) throws TransformException;

	/**
	 * Called only if {@link #getOutputPort(DataRecord)} throws an exception.
	 *
	 * @param exception an exception that caused {@link #getOutputPort(DataRecord)} to fail
	 * @param record data record which should be used for determining partition??? number
	 *
	 * @return port number which should be used for sending data out.
	 *
	 * @throws TransformException
	 */
	public int getOutputPortOnError(Exception exception, DataRecord record) throws TransformException;

	/**
	 * @param record data record which should be used for determining partition??? number
	 * @return port number which should be used for sending data out.
	 */
	public int getOutputPort(ByteBuffer directRecord) throws TransformException;

	/**
	 * Called only if {@link #getOutputPort(ByteBuffer)} throws an exception.
	 *
	 * @param exception an exception that caused {@link #getOutputPort(ByteBuffer)} to fail
	 * @param record data record which should be used for determining partition??? number
	 *
	 * @return port number which should be used for sending data out.
	 *
	 * @throws TransformException
	 */
	public int getOutputPortOnError(Exception exception, ByteBuffer directRecord) throws TransformException;

}
