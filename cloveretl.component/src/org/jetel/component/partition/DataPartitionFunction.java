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

import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;

/**
 * The base class for all partition functions.
 *
 * @author Lukas Krejci, Javlin a.s. &lt;lukas.krejci@javlin.eu&gt;
 *
 * @version 7th January 2010
 * @since 7th January 2010
 */
public abstract class DataPartitionFunction implements PartitionFunction {
	
	/** a transformation graph associated with this partition function */
	protected TransformationGraph graph;

	/** a graph node */
	protected Node node;

	/** how many partitions we have */
	protected int numPartitions;	
	/** a set of fields composing key based on which should the partition be determined */	   
	protected RecordKey partitionKey;	

	public final TransformationGraph getGraph() {
		return node != null ? node.getGraph() : graph;
	}
	
    /**
	 * Use setNode method.
	 */
    @Deprecated
	public final void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#getNode()
	 */
	public Node getNode() {
		return node;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#setNode(org.jetel.graph.Node)
	 */
	public void setNode(Node node) {
		this.node = node;
	}
	
	public final void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException {
		this.numPartitions = numPartitions;
		this.partitionKey = partitionKey;

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
	
	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
	}
}
