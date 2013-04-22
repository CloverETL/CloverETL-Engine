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

/**
 * Interface for the tracking information about component's output port. This interface should be used
 * by JMX clients.
 * 
 * Each change of this interface (rename, delete or add of an attribute) should be reflected in TrackingMetadataToolkit class.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2 Mar 2012
 */
public interface OutputPortTracking extends PortTracking {

	public static final PortType TYPE = PortType.OUTPUT;

	/**
	 * Available only in graph verbose mode.
	 * @return aggregated time how long the writer thread waits for data
	 */
	long getWriterWaitingTime();

}
