/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.graph.runtime;

import java.util.Properties;

/**
 * This class represents a graph runtime context. It is usually used like a parameter collection
 * for the Watchdog process.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 27.11.2007
 */
public interface IGraphRuntimeContext {
	
	/**
	 * Each implementation should be able to create copy of itself.
	 * It is used for creation of unconfigurable instance of context.
	 * @see UncofigurableGraphRuntimeContext
	 * @return deep copy of this instance
	 */
	public IGraphRuntimeContext createCopy();
	
	/**
	 * @return JMX tracking interval in milliseconds units
	 */
	public int getTrackingInterval();

    /**
     * @return determines whether JMX will be used during graph processing
     */
    public boolean useJMX();

    /**
     * @return determines whether a graph is processed in so called "verbose mode"
     */
    public boolean isVerboseMode();

    /**
     * @return true if only checkConfiguration() should be proceed (without graph processing)
     */
    public boolean isCheckConfig();
    
    /**
     * @return additional properties for processed graph
     */
    public Properties getAdditionalProperties();
    
}
