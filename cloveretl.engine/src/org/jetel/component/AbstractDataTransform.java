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
 * Base class of all transform stubs.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 14th June 2010
 * @created 14th June 2010
 */
public abstract class AbstractDataTransform implements Transform {

    /** A graph node associated with this CTL transform used to query graph, LUTs, sequences, etc.. */
	private Node node;
	/** A transformation graph associated with this transform. */
	protected TransformationGraph graph;

	/** Error message of an error that occurred during the transform. */
	protected String errorMessage = null;

    @Override
	public final void setNode(Node node) {
		this.node = node;

		// backwards compatibility
		if (node != null) {
			this.graph = node.getGraph();
		}
    }

	@Override
	public final Node getNode() {
		return node;
	}

	@Override
	public TransformationGraph getGraph() {
		return (node != null) ? node.getGraph() : graph;
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		// do nothing by default
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		errorMessage = null;
	}

	@Override
	public String getMessage() {
        return errorMessage;
	}

	/**
	 * @deprecated Use {@link #postExecute()} method.
	 */
	@Deprecated
	@Override
	public void finished(){
		// do nothing by default
	}

	/**
	 * @deprecated Use {@link #preExecute()} method.
	 */
	@Deprecated
	@Override
	public void reset() {
		// do nothing by default
	}

}
