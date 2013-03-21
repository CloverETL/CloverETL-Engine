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

import org.apache.commons.logging.Log;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.IGraphElement;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.string.StringUtils;

/**
 * Instances of this class are collected in ConfigurationStatus, which is return value of
 * all checkConfig() methods in the engine.
 * 
 * NOTE: {@link ConfigurationException} is tighly related class which should be updated respectivelly
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 24.11.2006
 * @see ConfigurationException
 */
public class ConfigurationProblem {
    
    private String message;
    
    private Severity severity;
    
    private Priority priority;
    
    private String attributeName;
    
    private String graphElementId;

    private String graphElementName;
    
    private Exception causeException;
    
	public ConfigurationProblem(String message, Severity severity, IGraphElement graphElement, Priority priority, String attributeName) {
        this.message = message;
        this.severity = severity;
        this.graphElementId = graphElement != null ? graphElement.getId() : null;
        this.graphElementName = graphElement != null ? graphElement.getName() : null;
        this.priority = priority;
        this.attributeName = attributeName;
    }
    
    public ConfigurationProblem(String message, Severity severity, IGraphElement graphElement, Priority priority) {
    	this(message, severity, graphElement, priority, null);
    }

    public ConfigurationProblem(Exception e, Severity severity, IGraphElement graphElement, Priority priority, String attributeName) {
    	this(null, e, severity,graphElement, priority, attributeName);
    }

    public ConfigurationProblem(String message, Exception e, Severity severity, IGraphElement graphElement, Priority priority, String attributeName) {
        this(message, severity, graphElement, priority, attributeName);
        
        setCauseException(e);
        
        if (e instanceof ComponentNotReadyException) {
	        if (!StringUtils.isEmpty(((ComponentNotReadyException) e).getAttributeName()) && StringUtils.isEmpty(attributeName)) {
	            setAttributeName(((ComponentNotReadyException) e).getAttributeName());
	        }
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
    
    public String getGraphElementID() {
    	return graphElementId;
	}

	public void setGraphElementID(String graphElementId) {
		this.graphElementId = graphElementId;
	}

    public String getGraphElementName() {
    	return graphElementName;
	}

    public Exception getCauseException() {
		return causeException;
	}

	public void setCauseException(Exception causeException) {
		this.causeException = causeException;
	}

	/**
	 * @return exception derived from this problem or null if the problem is not error
	 */
	public ConfigurationException toException() {
		if (getSeverity() == Severity.ERROR) {
			return new ConfigurationException(message, getCauseException(), getGraphElementID(), getGraphElementName(), getAttributeName());
		} else {
			return null;
		}
	}
	
	@Override
    public String toString() {
		ConfigurationException e = new ConfigurationException(message, getCauseException(), getGraphElementID(), getGraphElementName(), getAttributeName());
		return ExceptionUtils.stackTraceToString(e);
    }
	
}
