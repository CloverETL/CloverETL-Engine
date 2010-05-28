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
