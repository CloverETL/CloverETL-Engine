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
package org.jetel.component;

import java.util.Properties;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;

/**
 * Interface used by Java Execute component, which needs instance of this interface in order
 * to execute code require by it (by calling <i>run()</i> method).
 * For most instances, it is good, to extend BasicJavaRunnable.java class and just implement
 * run() method, the class does the rest.
 * 
 * @author jlehotsky
 * @created Dec 5, 2007
 *
 */
public interface JavaRunnable {
	
	/**
	 * Initializes java class/function. This method is called only once at the
	 * beginning of transformation process. Any object allocation/initialization should
	 * happen here.
	 *
	 * @param  parameters	   Global graph parameters and parameters defined specially for the
	 * component which calls this transformation class
	 * @return                  True if OK, otherwise False
	 */
	public boolean init(Properties parameters) throws ComponentNotReadyException;

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
     * @param transactionMethod type of transaction finalize method; was the graph/phase run successful?
     * @throws ComponentNotReadyException
     */
    public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException;

	/**
	 * The core method, which holds implementation of the Java code to be run by 
	 * Java Execute component. 
	 */
	public void run();
    
    /**
     * Method which passes into transformation graph instance within
     * which transformation will be executed.<br>
     * Since TransformationGraph singleton pattern was removed it is
     * NO longer POSSIBLE to access graph's parameters and other elements
     * (e.g. metadata definitions) through TransformationGraph.getIntance().
     * 
     * @param graph
     */
    public void setGraph(TransformationGraph graph);
	
    /**
     * Returns the transformation graph, in which class operates, previously
     * set by setGraph();
     * 
     * @return	TransformationGraph in which class operates.
     */
    public TransformationGraph getGraph();

}
