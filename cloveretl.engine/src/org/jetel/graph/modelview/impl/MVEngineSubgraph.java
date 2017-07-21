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
package org.jetel.graph.modelview.impl;

import java.util.Map;

import org.jetel.graph.TransformationGraph;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVSubgraph;
import org.jetel.util.SubgraphUtils;

/**
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 9. 2016
 */
public class MVEngineSubgraph extends MVEngineGraph implements MVSubgraph {

	private static final long serialVersionUID = 555858350437415235L;

	private MVComponent subgraphInputComponent;
	
	private MVComponent subgraphOutputComponent;
	
	public MVEngineSubgraph(TransformationGraph graph, MVComponent parentMVSubgraphComponent) {
		super(graph, parentMVSubgraphComponent);
	}

	@Override
	public MVComponent getSubgraphInputComponent() {
		if (subgraphInputComponent == null) {
			for (MVComponent component : getMVComponents().values()) {
				if (SubgraphUtils.isSubJobInputComponent(component.getModel().getType())) {
					subgraphInputComponent = component;
					break;
				}
			}
		}
		return subgraphInputComponent;
	}

	@Override
	public MVComponent getSubgraphOutputComponent() {
		if (subgraphOutputComponent == null) {
			for (MVComponent component : getMVComponents().values()) {
				if (SubgraphUtils.isSubJobOutputComponent(component.getModel().getType())) {
					subgraphOutputComponent = component;
					break;
				}
			}
		}
		return subgraphOutputComponent;
	}

	@Override
	public Map<Integer, MVEdge> getInputEdges() {
		return getSubgraphInputComponent().getOutputEdges();
	}

	@Override
	public Map<Integer, MVEdge> getOutputEdges() {
		return getSubgraphOutputComponent().getInputEdges();
	}

}
