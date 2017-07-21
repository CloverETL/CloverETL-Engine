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
package org.jetel.exception;

import org.jetel.graph.Node;

/**
 * A runtime exception thrown in TransformationGraph analysis
 * when recursive subgraph hierarchy is detected.
 * 
 * Stores the top-level Subgraph component
 * to enable error reporting in checkConfig().
 * 
 * @see <a href="https://bug.javlin.eu/browse/CLO-4930">CLO-4930</a>
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3. 10. 2014
 */
public class RecursiveSubgraphException extends JetelRuntimeException {

	private static final long serialVersionUID = -4176029858221446860L;
	
	private final Node node;

	/**
	 * Creates an instance of the exception with the specified message.
	 * The {@link Node} parameter is used to store
	 * the top-level Subgraph component instance for error reporting.
	 * 
	 * @param message	- error message
	 * @param node		- top-level Subgraph component
	 */
	public RecursiveSubgraphException(String message, Node node) {
		super(message);
		this.node = node;
	}

	/**
	 * Returns the top-level Subgraph component.
	 * 
	 * @return top-level Subgraph component
	 */
	public Node getNode() {
		return node;
	}

}
