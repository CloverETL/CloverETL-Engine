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
 * Interface for the tracking information about a graph node. This interface should be used
 * by JMX clients.
 * 
 * Each change of this interface (rename, delete or add of an attribute) should be reflected in TrackingMetadataToolkit class.
 * 
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
	 * @return total CPU time in milliseconds
	 * @see TrackingUtils#converTime(long, java.util.concurrent.TimeUnit) use this method for further time unit conversion
	 */
	long getTotalCPUTime();
	
	/**
	 * @return total user CPU time in milliseconds
	 * @see TrackingUtils#converTime(long, java.util.concurrent.TimeUnit) use this method for further time unit conversion
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
	InputPortTracking[] getInputPortTracking();
	
	/**
	 * @param portNumber
	 * @return tracking of the specified input port, or <code>null</code> if the port is not found.
	 */
	InputPortTracking getInputPortTracking(int portNumber);
	
	/**
	 * @return tracking of all output ports.
	 */
	OutputPortTracking[] getOutputPortTracking();
	
	/**
	 * @param portNumber
	 * @return tracking of the specified output port, or <code>null</code> if the port is not found.
	 */
	OutputPortTracking getOutputPortTracking(int portNumber);
	
}
