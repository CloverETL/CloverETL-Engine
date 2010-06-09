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

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * The base class for all rollup transformations.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 4th January 2010
 * @since 4th January 2010
 */
public abstract class DataRecordRollup implements RecordRollup {

	/** a transformation graph associated with this rollup transform */
	protected TransformationGraph graph;

	/** parameters used during the initialization of this rollup transform */
	protected Properties parameters;
	/** a metadata of input data records */
	protected DataRecordMetadata inputMetadata;
	/** a metadata of group accumulator or <code>null</code> if no group accumulator is used */
	protected DataRecordMetadata accumulatorMetadata;
	/** a metadata of output data records */
	protected DataRecordMetadata[] outputMetadata;

	/** an error message that occurred during this rollup transform */
	protected String errorMessage;

	public final void setGraph(TransformationGraph graph) {
    	this.graph = graph;
    }

    public final TransformationGraph getGraph() {
    	return graph;
    }

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

	/* (non-Javadoc)
	 * @see org.jetel.component.rollup.RecordRollup#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.rollup.RecordRollup#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
	}

    public final String getMessage() {
    	return errorMessage;
    }

    public void finished() {
    	// don't do anything
    }

    public void reset() throws ComponentNotReadyException {
    	errorMessage = null;
    }

}
