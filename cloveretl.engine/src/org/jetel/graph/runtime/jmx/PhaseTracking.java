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
package org.jetel.graph.runtime.jmx;

import java.io.Serializable;

import org.jetel.graph.Result;

/**
 * Interface for the tracking information about a phase. This interface should be used
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
public interface PhaseTracking extends Serializable {
	/**
	 * @return tracking of the phase's graph.
	 */
	GraphTracking getParentGraphTracking();
	
	/**
	 * @return phase number.
	 */
	int getPhaseNum();
	
	String getPhaseLabel();
	
	/**
	 * @return phase start time.
	 * @see System#currentTimeMillis()
	 */
	long getStartTime();
	
	/**
	 * @return phase end time.
	 * @see System#currentTimeMillis()
	 */
	long getEndTime();
	
	/**
	 * @return phase execution time in milliseconds.
	 * @see TrackingUtils#converTime(long, java.util.concurrent.TimeUnit) use this method for further time unit conversion
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
