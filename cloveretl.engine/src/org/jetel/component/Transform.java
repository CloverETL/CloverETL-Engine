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

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;

/**
 * Common transform interface which defines methods common to all transforms.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 14th June 2010
 * @created 14th June 2010
 */
public interface Transform {

    /**
     * Associates a graph <code>Node</code> with this transform.
     *
     * @param node a graph <code>Node</code> to be associated
     */
    public void setNode(Node node);

    /**
	 * @return a graph <code>Node</code> associated with this transform,
	 * or <code>null</code> if no graph node is associated
     */
    public Node getNode();

    /**
	 * @return a <code>TransformationGraph</code> associated with this transform,
	 * or <code>null</code> if no graph is associated
	 */
    public TransformationGraph getGraph();

    /**
     * Called during each graph run before the transform is executed. May be used to allocate and initialize resources
     * required by the transform. All resources allocated within this method should be released
     * by the {@link #postExecute()} method.
     *
     * @throws ComponentNotReadyException if an error occurred during resource allocation and initialization
     */
    public void preExecute() throws ComponentNotReadyException; 

    /**
     * Called during each graph run after the entire transform was executed. Should be used to free any resources
     * allocated within the {@link #preExecute()} method.
     *
     * @throws ComponentNotReadyException if an error occurred during resource clean-up
     */
    public void postExecute() throws ComponentNotReadyException;

    /**
     * Called to report any user-defined error message if an error occurred during the transform.
     *
     * @return a user-defined error message if an error occurred
     */
    public String getMessage();

	/**
     * Called at the end of the transform after all input data records were processed.
	 *
	 * @deprecated Use {@link #postExecute()} method.
	 */
	@Deprecated
	public void finished();

	/**
     * Resets the transform to the initial state (for another execution). This method may be called only
     * if the transform was successfully initialized before.
     *
	 * @deprecated Use {@link #preExecute()} method.
	 */
	@Deprecated
	public void reset();

}
