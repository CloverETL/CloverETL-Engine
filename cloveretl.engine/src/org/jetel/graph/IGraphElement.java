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

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;

/**
 * This interface should be implemented by all elements living in a transformation graph -
 * - components
 * - connections
 * - lookup tables
 * - sequences
 * - metadata
 * - ...
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.10.2009
 */
public interface IGraphElement {

    /**
     * Name of required XML attribute for element <i>identifier</i>.
     */
    public final static String XML_ID_ATTRIBUTE = "id";

    /**
     * Name of required XML attribute for element <i>type</i>.
     */
    public final static String XML_TYPE_ATTRIBUTE = "type";

    /**
     * Name of required XML attribute for element <i>name</i>.
     */
    public final static String XML_NAME_ATTRIBUTE = "name";

    /**
     * Check the element configuration.<br>
     * This method is called for each graph element before the graph is executed. This method should
     * verify that all required parameters are set and element may be use.
     */
    public ConfigurationStatus checkConfig(ConfigurationStatus status);

    /**
     * Initialization of the graph element. This initialization is done exactly once at the start
     * of existence the graph element. All resources, which are intended to be allocated all the time
     * of graph existence (including time between particular graph runs), should be allocated in this
     * method. All here allocated resources should be release in {@link #free()} method.
     *
     * @throws ComponentNotReadyException some of the required resource is not available or other
     * precondition is not accomplish
     */
    public void init() throws ComponentNotReadyException;

    /**
     * This is also initialization method, which is invoked before each separate graph run.
     * Contrary the init() procedure here should be allocated only resources for this graph run.
     * All here allocated resources should be released in #postExecute() method.
     * 
     * @throws ComponentNotReadyException some of the required resource is not available or other
     * precondition is not accomplish
     */
    public void preExecute() throws ComponentNotReadyException; 
        
    /**
     * This is de-initialization method for a single graph run. All resources allocated 
     * in {@link #preExecute()} method should be released here. It is guaranteed that this method
     * is invoked after graph finish at the latest. For some graph elements, for instance
     * components, is this method called immediately after phase finish.
     * 
     * @throws ComponentNotReadyException
     */
    public void postExecute() throws ComponentNotReadyException;
    
    /**
     * This method is called in case the graph run finished with success - Result.FINISHED_OK.
     * Graph elements should use this for tasks which should be done if and only if 
     * all graph processing was completely successful.
     * 
     * Each exception thrown from this method causes a FATAL error - the graph and all affected
     * systems are in unexpected state.
     * 
     * This method can be called only once the postExecute() method was already finished. Only
     * one of commit() and rollback() methods can be invoked.
     *  
     * NOTE: in the future this method will be called in the end of so called check-point area, what
     * is something like restart-ability point defined somewhere inside the graph. For now whole graph
     * forms one restart-ability area.
     */
    public void commit();
    
    /**
     * This method is called in case the graph run finished with error or was aborted - Result.ERROR, 
     * Result.ABORTED. Graph elements should use this for tasks which should be done if and only if 
     * some part of graph failed.
     * 
     * Each exception thrown from this method causes a FATAL error - the graph and all affected
     * systems are in unexpected state.
     * 
     * This method can be called only once the postExecute() method was already finished. Only
     * one of commit() and rollback() methods can be invoked.
     * 
     * NOTE: in the future this method will be called in the end of so called check-point area, what
     * is something like restart-ability point defined somewhere inside the graph. For now whole graph
     * forms one restart-ability area.
     */
    public void rollback();
    
    /**
     * @deprecated this method was substituted by pair of another methods {@link #preExecute()}
     * and {@link #postExecute()}.
     * 
     * This method should be invoked automatically by {@link #preExecute()} method for
     * backward compatibility. 
     * 
     * Restore all internal element settings to the initial state.
     * Cannot be called before init() and after free() method invokation. 
     */
    @Deprecated
    public void reset() throws ComponentNotReadyException;

    /**
     * This is de-initialization method for this graph element. All resources allocated
     * in {@link #init()} method should be released here. This method is invoked only once
     * at the end of element existence.
     */
    public void free();

    /**
     * @return transformation graph which this graph element belongs
     */
    public TransformationGraph getGraph();

    /**
     * Sets parent transformation graph.
     * @param graph
     */
    public void setGraph(TransformationGraph graph);

    /**
     * @return unique string identifier 
     */
    public String getId();

    /**
     * @return human readable graph element name
     */
    public String getName();

    /**
     * @param name human readable graph element name
     */
    public void setName(String name);

    /**
     * @return <b>true</b> if graph element is checked by checkConfig method; else <b>false</b>
     */
    public abstract boolean isChecked();

    /**
     * @return <b>true</b> if graph element is initialized by init() method; else <b>false</b>
     */
    public abstract boolean isInitialized();
    
    /**
     * @return true if the graph element is in first iteration of life cycle
     */
    public boolean firstRun();
    
}