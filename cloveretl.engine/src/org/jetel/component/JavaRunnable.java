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
import org.jetel.graph.TransformationGraph;
import org.jetel.util.CloverPublicAPI;

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
@CloverPublicAPI
public interface JavaRunnable extends Transform {
	
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
	 * The core method, which holds implementation of the Java code to be run by 
	 * Java Execute component. 
	 */
	public void run();
    
    /**
     * This is de-initialization method for this graph element. All resources allocated
     * in {@link #init()} method should be released here. This method is invoked only once
     * at the end of element existence.
     */
    public void free();
	
    /**
     * Method which passes into transformation graph instance within
     * which transformation will be executed.<br>
     * Since TransformationGraph singleton pattern was removed it is
     * NO longer POSSIBLE to access graph's parameters and other elements
     * (e.g. metadata definitions) through TransformationGraph.getIntance().
     * 
     * @param graph
     * @deprecated use {@link #setNode(org.jetel.graph.Node)} instead
     */
    public void setGraph(TransformationGraph graph);
	
}
