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

import java.util.HashMap;
import java.util.Map;

import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.SubgraphComponent;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVGraph;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * General model wrapper for engine transformation graph ({@link TransformationGraph}).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7. 3. 2014
 */
public class MVEngineGraph implements MVGraph {

	private TransformationGraph engineGraph;
	
	private Map<String, MVComponent> mvComponents;

	private Map<String, MVEdge> mvEdges;

	private Map<MVComponent, MVGraph> mvSubgraphs;
	
	private MVComponent parentMVSubgraphComponent;
	
	public MVEngineGraph(TransformationGraph graph, MVComponent parentMVSubgraphComponent) {
		if (graph == null) {
			throw new IllegalArgumentException("MVEngineGraph init failed");
		}
		this.engineGraph = graph;
		this.parentMVSubgraphComponent = parentMVSubgraphComponent;
		this.mvComponents = new HashMap<>();
		this.mvEdges = new HashMap<>();
	}
	
	@Override
	public TransformationGraph getModel() {
		return engineGraph;
	}
	
	@Override
	public String getId() {
		return engineGraph.getId();
	}
	
	@Override
	public void reset() {
		//reset all components
		for (MVComponent mvComponent : mvComponents.values()) {
			mvComponent.reset();
		}
		//reset all edges
		for (MVEdge mvEdge : mvEdges.values()) {
			mvEdge.reset();
		}
	}

	@Override
	public MVComponent getMVComponent(String componentId) {
		Node component = engineGraph.getNodes().get(componentId);
		if (component != null) {
			if (!mvComponents.containsKey(componentId)) {
				MVComponent mvComponent = new MVEngineComponent(component, this);
				mvComponents.put(componentId, mvComponent);
				return mvComponent;
			} else {
				return mvComponents.get(componentId);
			}
		} else {
			throw new IllegalArgumentException("unexpected component id, component does not exist in this graph");
		}
	}

	@Override
	public Map<MVComponent, MVGraph> getMVSubgraphs() {
		if (mvSubgraphs == null) {
			mvSubgraphs = new HashMap<>();
			for (Node component : engineGraph.getNodes().values()) {
				if (component instanceof SubgraphComponent) {
					TransformationGraph engineSubgraph = ((SubgraphComponent) component).getSubgraphNoMetadataPropagation(false);
					if (engineSubgraph != null) { //can be null if the subgraph component is not correctly defined - invalid subgraph URL 
						MVGraph mvSubgraph = new MVEngineGraph(engineSubgraph, getMVComponent(component.getId()));
						mvSubgraphs.put(getMVComponent(component.getId()), mvSubgraph);
					}
				}
			}
		}
		return mvSubgraphs;
	}

	@Override
	public MVEdge getMVEdge(String edgeId) {
		Edge edge = engineGraph.getEdges().get(edgeId);
		if (edge != null) {
			if (!mvEdges.containsKey(edgeId)) {
				MVEdge mvEdge = new MVEngineEdge(edge, this);
				mvEdges.put(edgeId, mvEdge);
				return mvEdge;
			} else {
				return mvEdges.get(edgeId);
			}
		} else {
			throw new IllegalArgumentException("unexpected edge id, edge does not exist in this graph");
		}
	}
	
	@Override
	public MVEdge getMVEdgeRecursive(Edge engineEdge) {
		if (engineEdge.getGraph() == engineGraph) {
			return getMVEdge(engineEdge.getId());
		} else {
			for (MVGraph mvSubgraph : getMVSubgraphs().values()) {
				MVEdge mvEdge = mvSubgraph.getMVEdgeRecursive(engineEdge);
				if (mvEdge != null) {
					return mvEdge;
				}
			}
			return null;
		}
	}

	@Override
	public MVGraph getMVGraphRecursive(TransformationGraph engineGraph) {
		if (this.engineGraph == engineGraph) {
			return this;
		} else {
			for (MVGraph mvSubgraph : getMVSubgraphs().values()) {
				MVGraph mvGraph = mvSubgraph.getMVGraphRecursive(engineGraph);
				if (mvGraph != null) {
					return mvGraph;
				}
			}
			return null;
		}
	}

	@Override
	public Map<String, MVEdge> getMVEdges() {
		return mvEdges;
	}
	
	@Override
	public MVMetadata createMVMetadata(DataRecordMetadata metadata) {
		return createMVMetadata(metadata, MVMetadata.DEFAULT_PRIORITY);
	}

	@Override
	public MVMetadata createMVMetadata(DataRecordMetadata metadata, int priority) {
		return new MVEngineMetadata(metadata, this, priority);
	}

	@Override
	public MVComponent getParentMVSubgraphComponent() {
		return parentMVSubgraphComponent;
	}

}
