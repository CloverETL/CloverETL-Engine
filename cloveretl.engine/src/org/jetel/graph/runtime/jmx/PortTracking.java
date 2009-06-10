package org.jetel.graph.runtime.jmx;

import java.io.Serializable;

/**
 * Interface for the tracking information about component's port. This interface should be used
 * by JMX clients.

 * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
 *         (c) Javlin Consulting (www.javlin.cz)
 *
 * @since May 14, 2009
 */
public interface PortTracking extends Serializable {
	/**
	 * @return tracking of the port's node.
	 */
	NodeTracking getParentNodeTracking();
	
	/**
	 * @return index of the port in the node.
	 */
	int getIndex();
	
	/**
	 * @return port type, value is either "Input" or "Output".
	 */
	String getType();
	
	/**
	 * @return current byte flow.
	 */
	int getByteFlow();
	
	/**
	 * @return maximum byte flow.
	 */
	int getBytePeak();
	
	/**
	 * @return total byte flow.
	 */
	long getTotalBytes();
	
	/**
	 * @return current record flow.
	 */
	int getRecordFlow();
	
	/**
	 * @return maximum record flow.
	 */
	int getRecordPeak();
	
	/**
	 * @return total record flow.
	 */
	int getTotalRecords();
	
	/**
	 * @return current record waiting for processing (cached).
	 */
	int getWaitingRecords();
	
	/**
	 * @return average waiting records.
	 */
	int getAverageWaitingRecords();
}
