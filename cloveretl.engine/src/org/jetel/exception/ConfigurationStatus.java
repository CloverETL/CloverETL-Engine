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

import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.GraphElement;

/**
 * This class is return value of all checkConfig() methods in the engine.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 24.11.2006
 */
public class ConfigurationStatus extends LinkedList<ConfigurationProblem> {

	private static final long serialVersionUID = -8680194056314131978L;
	
	private static Log logger = LogFactory.getLog(ConfigurationStatus.class);

    public enum Severity {
        ERROR, INFO, WARNING
    };
    
    public enum Priority {
        HIGH, NORMAL, LOW
    };

    public void log() {
        for(ConfigurationProblem problem : this) {
            problem.log(logger);
        }
    }
    
    public void add(String message, Severity severity, GraphElement graphElement, Priority priority, String attributeName) {
    	add(new ConfigurationProblem(message, severity, graphElement, priority, attributeName));
    }

    public void add(Exception e, Severity severity, GraphElement graphElement, Priority priority, String attributeName) {
    	add(new ConfigurationProblem(null, e, severity, graphElement, priority, attributeName));
    }

    public void add(String message, Exception e, Severity severity, GraphElement graphElement, Priority priority, String attributeName) {
    	add(new ConfigurationProblem(message, e, severity, graphElement, priority, attributeName));
    }

    public void add(String message, Severity severity, GraphElement graphElement, Priority priority) {
    	this.add(message, severity, graphElement, priority, null);
    }

    public void add(String message, Exception e, Severity severity, GraphElement graphElement, Priority priority) {
    	this.add(message, e, severity, graphElement, priority, null);
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
    
}
