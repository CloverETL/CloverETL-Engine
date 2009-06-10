package org.jetel.graph.runtime.jmx;

import java.io.Serializable;

import org.jetel.graph.Result;

/**
 * Interface for the tracking information about a phase. This interface should be used
 * by JMX clients.
 * 
 * TODO: currently the start and endtime is used by all clients only to calculate exec time - refactor.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
 *         (c) Javlin Consulting (www.javlin.cz)
 *
 * @since May 14, 2009
 */
public interface PhaseTracking extends Serializable {
	/**
	 * @return tracking of the phase's graph.
	 */
	GraphTracking getParentGraphTracking();
	
	/**
	 * @return phase number.
	 */
	int getPhaseNum();
	
	/**
	 * @return phase start time.
	 */
	long getStartTime();
	
	/**
	 * @return phase end time.
	 */
	long getEndTime();
	
	/**
	 * @return phase execution time.
	 */
	long getExecutionTime();
	
	
	/**
	 * @return current memory usage of the phase.
	 */
	long getMemoryUtilization();
	
	/**
	 * @return current result of the phase.
	 */
	Result getResult();
	
	
	/**
	 * @return tracking of all nodes of the phase.
	 */
	NodeTracking[] getNodeTracking();
	
	/**
	 * @param nodeID
	 * @return tracking of the specified node of the phase, or <code>null</code> if the node is not found.
	 */
	NodeTracking getNodeTracking(String nodeID);
}
