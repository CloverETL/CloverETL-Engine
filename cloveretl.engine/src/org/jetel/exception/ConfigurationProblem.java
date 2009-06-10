/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
package org.jetel.exception;

import org.apache.commons.logging.Log;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.GraphElement;
import org.jetel.util.string.StringUtils;

/**
 * Instances of this class are collected in ConfigurationStatus, which is return value of
 * all checkConfig() methods in the engine.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 24.11.2006
 */
public class ConfigurationProblem {
    
    private String message;
    
    private Severity severity;
    
    private GraphElement graphElement;

    private Priority priority;
    
    private String attributeName;
    
    private String graphElementID;
    
    public ConfigurationProblem(String message, Severity severity, GraphElement graphElement, Priority priority, String attributeName) {
        this.message = message;
        this.severity = severity;
        this.graphElement = graphElement;
        this.priority = priority;
        this.attributeName = attributeName;
    }
    
    public ConfigurationProblem(String message, Severity severity, GraphElement graphElement, Priority priority) {
    	this(message, severity, graphElement, priority, null);
    }

    public ConfigurationProblem(ComponentNotReadyException e, Severity severity, GraphElement graphElement, Priority priority, String attributeName) {
        this(e.getMessage(), severity, graphElement, priority, attributeName);
        
        if(!StringUtils.isEmpty(e.getAttributeName()) && StringUtils.isEmpty(attributeName)) {
            setAttributeName(e.getAttributeName());
        }
    }

    public void log(Log logger) {
        switch(severity) {
        case INFO:
            logger.info(this.toString());
            break;
        case WARNING:
            logger.warn(this.toString());
            break;
        case ERROR:
            logger.error(this.toString());
            break;
        }
    }
    
    public GraphElement getGraphElement() {
        return graphElement;
    }

    public String getMessage() {
        return message;
    }

    public Priority getPriority() {
        return priority;
    }

    public Severity getSeverity() {
        return severity;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }
    
    
    /**
     * @return ID of the associated graph element. It uses the associated graph element. When
     * the graph element is not specified, it uses the element ID specified via {@link #setGraphElementID(String)}.
     */
    public String getGraphElementID() {
    	if (graphElement != null) {
    		return graphElement.getId();
    	} else {
    		return graphElementID;
    	}
	}

	/**
	 * @param graphElementID ID of the associated graph element. Set this when you don't want to store
	 * the element here by itself (i.e. the graph is remote).
	 */
	public void setGraphElementID(String graphElementID) {
		this.graphElementID = graphElementID;
	}

	@Override
    public String toString() {
    	return (getGraphElement() != null ? 
    			(getGraphElement() + (getAttributeName() != null ? ("." + getAttributeName()) : "") + " - ") : "") + message;
    }
}
