/**
 * 
 */
package org.jetel.graph.runtime.jmx;

import java.io.Serializable;

import org.jetel.graph.Result;

/**
 * Interface for the tracking information about the whole graph. This interface should be used
 * by JMX clients.
 * 
 * TODO: currently the start and endtime is used by all clients only to calculate exec time - refactor.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
 *         (c) Javlin Consulting (www.javlin.cz)
 *
 * @since May 14, 2009
 */
public interface GraphTracking extends Serializable {
	/**
	 * @return graph start time.
	 */
	long getStartTime();
	
	/**
	 * @return graph end time.
	 */
	long getEndTime();
	
	/**
	 * @return graph execution time.
	 */
	long getExecutionTime();

	
	/**
	 * @return graph file name.
	 */
	String getGraphName();
	
	/**
	 * @return current graph result.
	 */
	Result getResult();

	
	/**
	 * @return tracking of all phases.
	 */
	PhaseTracking[] getPhaseTracking();
	
	/**
	 * @param phaseNum
	 * @return tracking of the specified phase, or <code>null</code> if the phase is not found.
	 */
	PhaseTracking getPhaseTracking(int phaseNum);
	
	/**
	 * @return tracking of the currently running phase.
	 */
	PhaseTracking getRunningPhaseTracking();
}
