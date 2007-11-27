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
 * Unconfigurable implementation of IGraphRuntimeContext interface.
 * It is usually used during graph processing where context should be static.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 27.11.2007
 */
public class UnconfigurableGraphRuntimeContext implements IGraphRuntimeContext {
	
	private IGraphRuntimeContext innerRuntimeContext;
	
	/**
	 * Creates unconfigurable wrapper over the given runtime context.
	 * @param runtimeContext
	 */
	public UnconfigurableGraphRuntimeContext(IGraphRuntimeContext runtimeContext) {
		innerRuntimeContext = runtimeContext.createCopy();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#getAdditionalProperties()
	 */
	public Properties getAdditionalProperties() {
		return innerRuntimeContext.getAdditionalProperties();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#getTrackingInterval()
	 */
	public int getTrackingInterval() {
		return innerRuntimeContext.getTrackingInterval();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#isCheckConfig()
	 */
	public boolean isCheckConfig() {
		return innerRuntimeContext.isCheckConfig();		
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#isVerboseMode()
	 */
	public boolean isVerboseMode() {
		return innerRuntimeContext.isVerboseMode();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#useJMX()
	 */
	public boolean useJMX() {
		return innerRuntimeContext.useJMX();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#createCopy()
	 */
	public IGraphRuntimeContext createCopy() {
		//it is not necessary to create copy object, since this implementation is already immutable
		return this;
	}

}
