package org.jetel.graph.runtime.jmx;

import java.io.Serializable;

import org.jetel.graph.Result;

/**
 * Interface for the tracking information about a graph node. This interface should be used
 * by JMX clients.

 * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
 *         (c) Javlin Consulting (www.javlin.cz)
 *
 * @since May 14, 2009
 */
public interface NodeTracking extends Serializable {
	/**
	 * @return tracking of the node's phase.
	 */
	PhaseTracking getParentPhaseTracking();
	
	/**
	 * @return node's graph ID.
	 */
	String getNodeID();

	/**
	 * @return node's name.
	 */
	String getNodeName();

	/**
	 * @return current CPU usage.
	 */
	float getUsageCPU();
	
	/**
	 * @return current user CPU usage.
	 */
	float getUsageUser();
	
	/**
	 * @return peak CPU usage.
	 */
	float getPeakUsageCPU();
	
	/**
	 * @return peak user CPU usage.
	 */
	float getPeakUsageUser();
	
	/**
	 * @return total CPU time.
	 */
	long getTotalCPUTime();
	
	/**
	 * @return total user CPU time.
	 */
	long getTotalUserTime();
	
	
	/**
	 * @return current result.
	 */
	Result getResult();
	
	
	/**
	 * @return <code>true</code> if the node has any ports.
	 */
	boolean hasPorts();
	
	/**
	 * @return tracking of all input ports.
	 */
	PortTracking[] getInputPortTracking();
	
	/**
	 * @param portNumber
	 * @return tracking of the specified input port, or <code>null</code> if the port is not found.
	 */
	PortTracking getInputPortTracking(int portNumber);
	
	/**
	 * @return tracking of all output ports.
	 */
	PortTracking[] getOutputPortTracking();
	
	/**
	 * @param portNumber
	 * @return tracking of the specified output port, or <code>null</code> if the port is not found.
	 */
	PortTracking getOutputPortTracking(int portNumber);
}
