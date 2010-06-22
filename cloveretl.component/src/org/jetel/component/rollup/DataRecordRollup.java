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
package org.jetel.component.rollup;

import java.util.Properties;

import org.jetel.component.AbstractDataTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * The base class for all rollup transformations.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 18th June 2010
 * @since 4th January 2010
 */
public abstract class DataRecordRollup extends AbstractDataTransform implements RecordRollup {

	/** parameters used during the initialization of this rollup transform */
	protected Properties parameters;
	/** a metadata of input data records */
	protected DataRecordMetadata inputMetadata;
	/** a metadata of group accumulator or <code>null</code> if no group accumulator is used */
	protected DataRecordMetadata accumulatorMetadata;
	/** a metadata of output data records */
	protected DataRecordMetadata[] outputMetadata;

    public final void init(Properties parameters, DataRecordMetadata inputMetadata, DataRecordMetadata accumulatorMetadata,
            DataRecordMetadata[] outputMetadata) throws ComponentNotReadyException {
    	this.parameters = parameters;
    	this.inputMetadata = inputMetadata;
    	this.accumulatorMetadata = accumulatorMetadata;
    	this.outputMetadata = outputMetadata;

    	init();
    }

    /**
     * Override this method to provide user-desired initialization of this rollup transform. This method is called
     * from the {@link #init(Properties, DataRecordMetadata, DataRecordMetadata, DataRecordMetadata[])} method.
     *
     * @throws ComponentNotReadyException if the initialization fails for any reason
     */
    protected void init() throws ComponentNotReadyException {
    	// don't do anything
    }

	@Override
	public void initGroupOnError(Exception exception, DataRecord inputRecord, DataRecord groupAccumulator)
			throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Rollup failed!", exception);
	}

	@Override
	public boolean updateGroupOnError(Exception exception, DataRecord inputRecord, DataRecord groupAccumulator)
			throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Rollup failed!", exception);
	}

	@Override
	public boolean finishGroupOnError(Exception exception, DataRecord inputRecord, DataRecord groupAccumulator)
			throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Rollup failed!", exception);
	}

	@Override
	public int updateTransformOnError(Exception exception, int counter, DataRecord inputRecord,
			DataRecord groupAccumulator, DataRecord[] outputRecords) throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Rollup failed!", exception);
	}

	@Override
	public int transformOnError(Exception exception, int counter, DataRecord inputRecord, DataRecord groupAccumulator,
			DataRecord[] outputRecords) throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Rollup failed!", exception);
	}

}
