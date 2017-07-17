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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.GraphElement;
import org.jetel.graph.IGraphElement;
import org.jetel.util.CloverPublicAPI;
import org.jetel.util.string.StringUtils;

/**
 * This class is return value of all checkConfig() methods in the engine.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 24.11.2006
 */
@CloverPublicAPI
public class ConfigurationStatus implements Iterable<ConfigurationProblem> {

	private static Log logger = LogFactory.getLog(ConfigurationStatus.class);

    public enum Severity {
        ERROR, INFO, WARNING
    };
    
    public enum Priority {
        HIGH, NORMAL, LOW
    };

    private LinkedList<ConfigurationProblem> configurationProblems = new LinkedList<>();

    public ConfigurationProblem addError(IGraphElement graphElement, String attributeName, String message) {
    	return addError(graphElement, attributeName, message, null);
    }
    
    public ConfigurationProblem addError(IGraphElement graphElement, String attributeName, Throwable cause) {
    	return addError(graphElement, attributeName, null, cause);
    }
    
    public ConfigurationProblem addError(IGraphElement graphElement, String attributeName, String message, Throwable cause) {
    	return addProblem(graphElement, attributeName, Severity.ERROR, message, cause);
    }

    public ConfigurationProblem addWarning(IGraphElement graphElement, String attributeName, String message) {
    	return addWarning(graphElement, attributeName, message, null);
    }
    
    public ConfigurationProblem addWarning(IGraphElement graphElement, String attributeName, Throwable cause) {
    	return addWarning(graphElement, attributeName, null, cause);
    }
    
    public ConfigurationProblem addWarning(IGraphElement graphElement, String attributeName, String message, Throwable cause) {
    	return addProblem(graphElement, attributeName, Severity.WARNING, message, cause);
    }

    public ConfigurationProblem addInfo(IGraphElement graphElement, String attributeName, String message) {
    	return addInfo(graphElement, attributeName, message, null);
    }
    
    public ConfigurationProblem addInfo(IGraphElement graphElement, String attributeName, Throwable cause) {
    	return addInfo(graphElement, attributeName, null, cause);
    }
    
    public ConfigurationProblem addInfo(IGraphElement graphElement, String attributeName, String message, Throwable cause) {
    	return addProblem(graphElement, attributeName, Severity.INFO, message, cause);
    }

    /**
     * CLO-10929:
     * Use this method only if you need to set Severity dynamically.
     * Otherwise use one of the dedicated methods above.
     * 
     * @param graphElement
     * @param attributeName
     * @param severity
     * @param message
     * @param cause
     * 
     * @return
     */
    public ConfigurationProblem addProblem(IGraphElement graphElement, String attributeName, Severity severity, String message, Throwable cause) {
    	ConfigurationProblem configurationProblem = new ConfigurationProblem(message, severity, graphElement, Priority.NORMAL, attributeName);
    	configurationProblems.add(configurationProblem);
    	configurationProblem.setAttributeName(attributeName);
    	configurationProblem.setCauseException(cause);
    	if (StringUtils.isEmpty(attributeName)
    			&& cause instanceof ComponentNotReadyException
    			&& !StringUtils.isEmpty(((ComponentNotReadyException) cause).getAttributeName())) {
    		configurationProblem.setAttributeName(((ComponentNotReadyException) cause).getAttributeName());
    	}
    	return configurationProblem;
    }

    public void joinWith(ConfigurationStatus otherStatus) {
    	configurationProblems.addAll(otherStatus.configurationProblems);
    }
    
    @Deprecated
    public void add(String message, Severity severity, GraphElement graphElement, Priority priority, String attributeName) {
    	add(new ConfigurationProblem(message, severity, graphElement, priority, attributeName));
    }

    @Deprecated
    public void add(Exception e, Severity severity, GraphElement graphElement, Priority priority, String attributeName) {
    	add(new ConfigurationProblem(null, e, severity, graphElement, priority, attributeName));
    }

    @Deprecated
    public void add(String message, Exception e, Severity severity, GraphElement graphElement, Priority priority, String attributeName) {
    	add(new ConfigurationProblem(message, e, severity, graphElement, priority, attributeName));
    }

    @Deprecated
    public void add(String message, Severity severity, GraphElement graphElement, Priority priority) {
    	this.add(message, severity, graphElement, priority, null);
    }

    @Deprecated
    public void add(String message, Exception e, Severity severity, GraphElement graphElement, Priority priority) {
    	this.add(message, e, severity, graphElement, priority, null);
    }

    public ConfigurationProblem add(ConfigurationProblem configurationProblem) {
    	configurationProblems.add(configurationProblem);
    	return configurationProblem;
    }

    /**
     * @return true if status contains a problem with the ERROR severity
     */
    public boolean isError() {
    	for(ConfigurationProblem problem : this) {
    		if(problem.getSeverity() == Severity.ERROR) {
    			return true;
    		}
    	}
    	
    	return false;
    }
    
    /**
     * @return first configuration problem with <code>error</code> severity
     */
    public ConfigurationProblem firstError() {
    	for (ConfigurationProblem problem : this) {
    		if (problem.getSeverity() == Severity.ERROR) {
    			return problem;
    		}
    	}
    	
    	return null;
    }

	/**
	 * @return exception derived from first error in status or null if no error is in status
	 */
	public ConfigurationException toException() {
		if (isError()) {
			return firstError().toException();
		} else {
			return null;
		}
	}
    
    public void log() {
        for(ConfigurationProblem problem : this) {
            problem.log(logger);
        }
    }

	@Override
	public Iterator<ConfigurationProblem> iterator() {
		return configurationProblems.iterator();
	}

	public ConfigurationProblem getFirstProblem() {
		return configurationProblems.getFirst();
	}

	public ConfigurationProblem getLastProblem() {
		return configurationProblems.getLast();
	}

	public boolean hasProblem() {
		return !configurationProblems.isEmpty();
	}

	public List<ConfigurationProblem> getProblems() {
		return new ArrayList<>(configurationProblems);
	}

	public void clear() {
		configurationProblems.clear();
	}
	
}
