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
package org.jetel.component.denormalize;

import java.util.Properties;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Base class for various denormalization implementations.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06  
 * @see org.jetel.component.Denormalizer
 */
public abstract class DataRecordDenormalize implements RecordDenormalize {

	protected Properties parameters;
	protected DataRecordMetadata sourceMetadata;
	protected DataRecordMetadata targetMetadata;
		
	protected String errorMessage;
	private TransformationGraph graph;

	/**
	 * Use postExecuste method.
	 */
	@Deprecated
	public void finished() {
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#init(org.jetel.graph.TransformationGraph, java.util.Properties, org.jetel.metadata.DataRecordMetadata, org.jetel.metadata.DataRecordMetadata)
	 */
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		this.parameters = parameters;
		this.sourceMetadata = sourceMetadata;
		this.targetMetadata = targetMetadata;
		this.errorMessage = null;
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.denormalize.RecordDenormalize#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.denormalize.RecordDenormalize#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#getMessage()
	 */
	public String getMessage() {
		return errorMessage;
	}

	
	/* (non-Javadoc)
	 * @see org.jetel.component.denormalize.RecordDenormalize#clean()
	 */
	public void clean(){
		//do nothing - by default
	}
	
	/**
	 * Use preExecute method.
	 */
	@Deprecated
	public void reset() {
		errorMessage = null;
	}
	
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public TransformationGraph getGraph() {
		return graph;
	}

}
