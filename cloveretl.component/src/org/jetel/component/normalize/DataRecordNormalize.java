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
package org.jetel.component.normalize;

import java.util.Properties;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Base class for various normalization implementations.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06  
 * @see org.jetel.component.Denormalizer
 */
public abstract class DataRecordNormalize implements RecordNormalize {

	protected Properties parameters;
	protected DataRecordMetadata sourceMetadata;
	protected DataRecordMetadata targetMetadata;
		
	protected String errorMessage;
	private TransformationGraph graph;

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#finished()
	 */
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
	 * @see org.jetel.component.RecordNormalize#getMessage()
	 */
	public String getMessage() {
		return errorMessage;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#finalize()
	 */
	public void clean(){
		//do nothing
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.component.normalize.RecordNormalize#reset()
	 */
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
