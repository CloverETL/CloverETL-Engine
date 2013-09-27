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

import org.apache.log4j.Logger;
import org.jetel.data.GraphElementDescription;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.util.property.PropertyRefResolver;

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
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
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
     * Checks the configuration of this graph element.
     * This method should verify that all required parameters are set and that the element can be used.
     * 
     * This method might be called before graph execution,
     * in such case it will be called before the {@link #init()} method.
     * It is not guaranteed that this method will ever be called, but also this method may be called repeatedly.
     * All resources allocated in this method should be freed in this method too.
     */
    public ConfigurationStatus checkConfig(ConfigurationStatus status);

    /**
     * Initialization of the graph element. This initialization is done exactly once at the start
     * of the existence of this graph element. All resources which are intended to remain allocated during the whole time
     * of the graph existence (including time between each graph run), should be allocated in this
     * method. All resources allocated in this method should be released in the {@link #free()} method.
     *
     * @throws ComponentNotReadyException some of the required resource is not available or other
     * precondition is not accomplish
     */
    public void init() throws ComponentNotReadyException;

    /**
     * This is also initialization method, which is invoked before each separate graph run. That is,
     * it can be invoked multiple times. Use {@link #firstRun()} to determine whether given invocation is the first one.
     * Contrary to the {@link #init()} procedure, this method should allocate only resources for this single graph run.
     * All resources allocated in this method should be released in the {@link #postExecute()} method.
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
     * This method will be called only once the postExecute() method was already finished. Only
     * one of commit() and rollback() methods will be invoked.
     *  
     * NOTE: in the future this method will be called in the end of so called check-point area, which
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
     * This method will be called only once the postExecute() method was already finished. Only
     * one of commit() and rollback() methods will be invoked.
     * 
     * NOTE: in the future this method will be called in the end of so called check-point area, which
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
     * Cannot be called before init() and after free() method invocation. 
     */
    @Deprecated
    public void reset() throws ComponentNotReadyException;

    /**
     * This is de-initialization method for this graph element. All resources allocated
     * in {@link #init()} method should be released here. This method is invoked only once
     * at the end of this graph element existence.
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
    
    /**
     * Returns kind of parent graph - {@link JobType#ETL_GRAPH} or {@link JobType#JOBFLOW}.
     * The transformation graph can be driven in slightly different way in case jobflow run.  
     */
    public JobType getJobType();

    /**
     * @return dedicated logger for this graph element
     */
    public Logger getLog();
    
    /**
     * This methods is easy way how to get valid property reference resolver,
     * which can be used to resolve graph parameters (${x}), special characters (\n) and
     * in-line CTL code (`today()`) in a given string.
     * @return property reference resolver populated with parameters of parent graph
     */
    public PropertyRefResolver getPropertyRefResolver();
    
    /**
     * @return element type descriptor
     */
    public GraphElementDescription getDescription();
    
    /**
     * Sets element type descriptor.
     * @param description descriptor for type of this element
     */
    public void setDescription(GraphElementDescription description);
    
}