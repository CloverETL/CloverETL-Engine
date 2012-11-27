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
 * Each change of this interface (rename, delete or add of an attribute) should be reflected in TrackingMetadataToolkit class.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
 *         (c) Javlin Consulting (www.javlin.cz)
 *
 * @since May 14, 2009
 */
public interface GraphTracking extends Serializable {

	/**
	 * @return graph start 
	 * @see System#currentTimeMillis()
	 */
	long getStartTime();
	
	/**
	 * @return graph end time.
	 * @see System#currentTimeMillis()
	 */
	long getEndTime();
	
	/**
	 * @return graph execution time in milliseconds.
	 * @see TrackingUtils#converTime(long, java.util.concurrent.TimeUnit) use this method for further time unit conversion
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
	 * @return identifier of cluster node, where the graph was executed; can be null for non-cluster runs
	 */
	String getNodeId();
	
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
	
	/**
	 * @return size of memory footprint for whole graph (not guaranteed)
	 */
	int getUsedMemory();

}
