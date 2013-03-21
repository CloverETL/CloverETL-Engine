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
package org.jetel.exception;

import org.jetel.graph.GraphElement;


/**
 * Exception derived from {@link ConfigurationProblem}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7.6.2012
 * 
 * @see ConfigurationProblem#toException()
 */
public class ConfigurationException extends Exception {

	private static final long serialVersionUID = -4610905816299033384L;
	
	private String causedGraphElementId;
	private String causedGraphElementName;
	private String attributeName;
	
	public ConfigurationException(String message, Throwable cause, String causedGraphElementId, String causedGraphElementName, String attributeName) {
		super(new JetelRuntimeException(message, cause));
		
		this.causedGraphElementId = causedGraphElementId;
		this.causedGraphElementName = causedGraphElementName;
		this.attributeName = attributeName;
	}

	/**
	 * @return the causedGraphElementId
	 */
	public String getCausedGraphElementId() {
		return causedGraphElementId;
	}

	@Override
	public String getMessage() {
    	if (getCausedGraphElementId() != null) {
    		StringBuilder result = new StringBuilder();
    		result.append("Issue in component " + GraphElement.identifiersToString(causedGraphElementId, causedGraphElementName) + (attributeName != null ? ("." + attributeName) : ""));
        	return result.toString();
    	} else {
    		return null;
    	}
	}

}
