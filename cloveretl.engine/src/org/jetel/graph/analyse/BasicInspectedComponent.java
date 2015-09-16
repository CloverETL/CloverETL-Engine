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
package org.jetel.graph.analyse;

import org.jetel.graph.Edge;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;

/**
 * Component wrapper for basic graph.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.12.2012
 * @see GraphCycleInspector
 * @see SingleGraphProvider
 */
public class BasicInspectedComponent extends AbstractInspectedComponent {
	
	/**
	 * @param component
	 */
	public BasicInspectedComponent(GraphProvider graphProvider, Node component, Edge excludedEdge) {
		super(graphProvider, component, excludedEdge);
	}

	@Override
	protected InspectedComponent getNextComponent(OutputPort outputPort) {
		if (isOutputEdgeAllowed(outputPort.getEdge())) {
			return createInspectedComponent(outputPort.getReader(), outputPort.getEdge());
		} else {
			return null;
		}
	}

	@Override
	protected InspectedComponent getNextComponent(InputPort inputPort) {
		if (isInputEdgeAllowed(inputPort.getEdge())) {
			return createInspectedComponent(inputPort.getWriter(), inputPort.getEdge());
		} else {
			return null;
		}
	}

	@Override
	protected InspectedComponent createInspectedComponent(Node component, Edge entryEdge) {
		return new BasicInspectedComponent(graphProvider, component, entryEdge);
	}
	
}
