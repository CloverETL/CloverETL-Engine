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
package org.jetel.component;

import org.jetel.exception.ConfigurationStatus;
import org.jetel.util.CloverPublicAPI;


/**
 * Transformation for GenericComponent.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 11. 2014
 */
@CloverPublicAPI
public interface GenericTransform extends Transform {
	
	/**
	 * Initialization method called during graph initialization.
	 */
	public void init();
	
	/**
	 * Main execution method, called once per component run.
	 */
	public void execute() throws Exception;
	
	/**
	 * Called when {@link #execute()} throws exception.
	 * Implementors can e.g. log a custom message or try to recover from the exception.
	 * @param e The exception thrown by {@link #execute()}
	 */
	public void executeOnError(Exception e);
	
	/**
     * @see org.jetel.graph.IGraphElement#free()
     */
	public void free();

	/**
	 * Allows implementing custom configuration checking. Called during graph configuration check.
	 * @param status Configuration status of the graph.
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status);
	
}
