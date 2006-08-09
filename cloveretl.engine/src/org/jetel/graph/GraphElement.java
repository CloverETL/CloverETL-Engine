/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.graph;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.util.StringUtils;
import org.w3c.dom.Element;


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
public abstract class GraphElement {

    /**
     * XML attribute of every cloverETL element.
     */
    public final static String XML_ID_ATTRIBUTE = "id";
    public final static String XML_TYPE_ATTRIBUTE = "type";

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
    
    /**
     * Check the element configuration.<br>
     * This method is called for each graph element before the graph is executed. This method should
     * verify that all required parameters are set and element may be use.
     * @return    <b>true</b> if element configuration is OK, otherwise <b>false</b>
     */
    public abstract boolean checkConfig();

    /**
     *  Initialization of Node
     *
     *@exception  ComponentNotReadyException  Error when trying to initialize
     *      Node/Component
     *@since                                  April 2, 2002
     */
    public abstract void init() throws ComponentNotReadyException;

    /**
     * Free all allocated resources.
     */
    public abstract void free();
    
    public TransformationGraph getGraph() {
        return graph;
    }

    public void setGraph(TransformationGraph graph) {
        this.graph = graph;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public static GraphElement fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
        throw new UnsupportedOperationException("not implemented in org.jetel.graph.GraphElement"); 
    }
    
    public abstract void toXML(Element xmlElement);
}
