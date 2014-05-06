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
package org.jetel.graph.parameter;

import org.jetel.component.Transform;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;

/**
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29. 4. 2014
 */
public interface GraphParameterValueFunction extends Transform {

	public static final String GET_PARAMETER_VALUE_FUNCTION_NAME = "getValue";
	public static final String INIT_FUNCTION_NAME = "init";
	
	/**
	 * Called before {@link #getParameterValue()} is called for the first time.
	 * @throws ComponentNotReadyException 
	 */
	public void init() throws ComponentNotReadyException;
	
	/**
	 * Graph parameter dynamic value as a string.
	 * 
	 * @return
	 * @throws TransformException 
	 */
	public String getValue() throws TransformException;

}
