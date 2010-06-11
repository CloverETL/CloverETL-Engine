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
package org.jetel.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;


/**
 * Ascendant of all elements, which can be put into transformation graph.
 * All elements have reference to appropriate graph, unique identifier and name (description).
 * 
 * And three methods, which may be called in this order:
 *      checkConfig();  //checks whether has element valid parameters.
 *      init();         //allocate all necessarily resources
 *      reset();		//can be called repeatedly to reset element to initial state
 *      free();         //free all allocated resources 
 * 
 * @author Martin Zatopek
 */
public abstract class GraphElement implements IGraphElement {

    private static Log logger = LogFactory.getLog(GraphElement.class);

    final private String id;

    private TransformationGraph graph;
    
    private String name;

    private boolean checked;

    private boolean initialized;
    
    /**
     * This variable is here just for backward compatibility. Deprecated {@link #reset()} method
     * cannot be invoked in first run of transformation graph. 
     */
    private boolean firstRun = true;
    
    /**
     * Constructor.
     * @param id
     */
    public GraphElement(String id) {
        this(id, null, null);
    }

    /**
     * Constructor.
     * @param id
     * @param graph
     */
    public GraphElement(String id, TransformationGraph graph) {
        this(id, graph, null);
    }

    /**
     * Constructor.
     * @param id
     * @param name
     */
    public GraphElement(String id, String name) {
        this(id, null, name);
    }

    /**
     * Constructor.
     * @param id
     * @param graph
     * @param name
     */
    public GraphElement(String id, TransformationGraph graph, String name) {
        if (id != null && !StringUtils.isValidObjectId(id)) {
        	throw new InvalidGraphObjectNameException(id, "GRAPH_ELEMENT");
        }
        this.id = id;
        this.graph = graph;
        this.name = name;
        this.checked = false;
        this.initialized = false;
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#checkConfig()
     */
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        checked = true;
        return status;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#init()
     */
    synchronized public void init() throws ComponentNotReadyException {
        initialized = true;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#preExecute()
     */
    synchronized public void preExecute() throws ComponentNotReadyException {
    	if (!firstRun) {
    		//this is necessary for backward compatibility for graph elements 
			//with obsoleted reset() method already implemented
    		reset(); 
    	}
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#postExecute()
     */
    public void postExecute() throws ComponentNotReadyException {
    	firstRun = false;
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#commit()
     */
    @Override
    public void commit() {
    	//default empty implementation
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#rollback()
     */
    @Override
    public void rollback() {
    	//default empty implementation
    }
    
    /**
     * @see org.jetel.graph.IGraphElement#reset()
     * @deprecated see {@link org.jetel.graph.IGraphElement#preExecute()} and {@link org.jetel.graph.IGraphElement#postExecute()} methods 
     */
    @Deprecated
    synchronized public void reset() throws ComponentNotReadyException {
        if(!isInitialized()) {
        	String msg = "Graph element " + this + " is not initialized, cannot be reset before initialization or after element was released.";
            logger.error(msg);
        	throw new IllegalStateException(msg);
        }
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#free()
     */
    synchronized public void free() {
        initialized = false;
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#getGraph()
     */
    public TransformationGraph getGraph() {
        return graph;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#setGraph(org.jetel.graph.TransformationGraph)
     */
    public void setGraph(TransformationGraph graph) {
        this.graph = graph;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#getId()
     */
    public String getId() {
        return id;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#getName()
     */
    public String getName() {
        return name;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#setName(java.lang.String)
     */
    public void setName(String name) {
        this.name = name;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#isChecked()
     */
    public boolean isChecked() {
        return checked;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#isInited()
     */
    public boolean isInitialized() {
        return initialized;
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#firstRun()
     */
    public boolean firstRun() {
    	return firstRun;
    }
    
    @Override
    public String toString() {
        return (getName() == null ? "" : getName()) + "[" + getId() + "]";
    }
    
	/**
	 * Only purpose of this implementation is obfuscation. 
	 * Method fromXML() should not be obfuscated in all descendants.
	 */
	public static GraphElement fromXML(TransformationGraph graph, Element xmlElement)throws XMLConfigurationException {
        throw new UnsupportedOperationException("not implemented in org.jetel.graph.GraphElement"); 
	}

}
