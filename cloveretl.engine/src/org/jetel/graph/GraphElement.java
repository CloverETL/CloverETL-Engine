/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.graph;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.util.StringUtils;


/**
 * Ascendant of all elements, which can be put into transformation graph.
 * All elements have reference to appropriate graph, unique identifier and name (description).
 * 
 * And three methods, which may be called in this order:
 *      checkConfig();  //checks whether has element valid parameters.
 *      init();         //allocate all necessarily resources
 *      free();         //free all allocated resources 
 * 
 * @author Martin Zatopek
 */
public abstract class GraphElement implements IGraphElement {

    final private String id;

    private TransformationGraph graph;
    
    private String name;

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
    }
    
    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#checkConfig()
     */
    public abstract ConfigurationStatus checkConfig(ConfigurationStatus status);

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#init()
     */
    public abstract void init() throws ComponentNotReadyException;

    /* (non-Javadoc)
     * @see org.jetel.graph.IGraphElement#free()
     */
    public abstract void free();
    
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
    
}
