/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 * $Header$
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
        if(id != null || graph != null) {
            if (!StringUtils.isValidObjectName(id)){
                throw new InvalidGraphObjectNameException(id, "GRAPH_ELEMENT");
            }
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
        if(!isChecked()) {
            logger.warn("Graph element " + this + " is not checked by checkConfig() method. Please call TransformationGraph.checkConfig() first.");
        }
    }

    synchronized public void reset() throws ComponentNotReadyException {
        if(!isInitialized()) {
        	String msg = "Graph element " + this + " is not initialized, cannot be reseted before initialization or after element was released.";
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
    
    @Override
    public String toString() {
        return (getName() == null ? "" : getName()) + "[" + getId() + "]";
    }
    
	/**
	 * Only purpose of this implementation is obfuscation. 
	 * Method fromXML() should not be obfuscated in all descendants.
	 */
	public static IGraphElement fromXML(TransformationGraph graph, Element xmlElement)throws XMLConfigurationException {
        throw new UnsupportedOperationException("not implemented in org.jetel.graph.GraphElement"); 
	}

}
