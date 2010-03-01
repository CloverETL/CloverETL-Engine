package org.jetel.component.partition;

import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
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
	
	/** how many partitions we have */
	protected int numPartitions;	
	/** a set of fields composing key based on which should the partition be determined */	   
	protected RecordKey partitionKey;	

	public final TransformationGraph getGraph() {
		return graph;
	}
	
	public final void setGraph(TransformationGraph graph) {
		this.graph = graph;
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
}
