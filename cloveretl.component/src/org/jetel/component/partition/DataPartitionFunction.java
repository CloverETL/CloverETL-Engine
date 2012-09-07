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
import java.util.Properties;

import org.jetel.component.AbstractDataTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * The base class for all partition functions.
 * 
 * @author Lukas Krejci, Javlin a.s. &lt;lukas.krejci@javlin.eu&gt;
 * 
 * @version 17th June 2010
 * @since 7th January 2010
 */
public abstract class DataPartitionFunction extends AbstractDataTransform implements PartitionFunction {

	/** how many partitions we have */
	protected int numPartitions;
	/** a set of fields composing key based on which should the partition be determined */
	protected RecordKey partitionKey;
	/** user-specified parameters passed via custom component attributes */
	protected Properties parameters;
	/** metadata of partitioned data */
	protected DataRecordMetadata metadata;
	
	@Override
	@Deprecated
	public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException {
		init(numPartitions, partitionKey, null, null);
	}
	
	@Override
	public final void init(int numPartitions, RecordKey partitionKey, Properties parameters, DataRecordMetadata metadata) throws ComponentNotReadyException {
		this.numPartitions = numPartitions;
		this.partitionKey = partitionKey;
		this.parameters = parameters;
		this.metadata = metadata;

		init();
	}

	/**
	 * Override this method to provide user-desired initialization of this partition function. This method is called
	 * from the {@link #init(int numPartitions, RecordKey partitionKey)} method.
	 * 
	 * @throws ComponentNotReadyException if the initialization fails for any reason
	 */
	protected void init() throws ComponentNotReadyException {
		// don't do anything
	}

	@Override
	public int getOutputPortOnError(Exception exception, DataRecord record) throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Partitioning failed!", exception);
	}

	@Override
	@Deprecated
	public int getOutputPortOnError(Exception exception, ByteBuffer directRecord) throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Partitioning failed!", exception);
	}

	@Override
	public int getOutputPortOnError(Exception exception, CloverBuffer directRecord) throws TransformException {
		return getOutputPortOnError(exception, directRecord.buf());
		
//following lines are correct implementation after obsolete function with ByteBuffers will be removed
//		// by default just throw the exception that caused the error
//		throw new TransformException("Partitioning failed!", exception);
	}

	@Override
	@Deprecated
	public int getOutputPort(ByteBuffer directRecord) throws TransformException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getOutputPort(CloverBuffer directRecord) throws TransformException {
		return getOutputPort(directRecord.buf());
	}

}
