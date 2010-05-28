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
