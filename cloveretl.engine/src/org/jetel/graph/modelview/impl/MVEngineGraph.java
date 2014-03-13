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

	private TransformationGraph graph;
	
	private Map<String, MVComponent> mvComponents;

	private Map<String, MVEdge> mvEdges;

	private Map<MVComponent, MVGraph> mvSubgraphs;
	
	public MVEngineGraph(TransformationGraph graph) {
		this.graph = graph;
		this.mvComponents = new HashMap<>();
		this.mvEdges = new HashMap<>();
	}
	
	@Override
	public TransformationGraph getModel() {
		return graph;
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
		Node component = graph.getNodes().get(componentId);
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
			for (Node component : graph.getNodes().values()) {
				if (component instanceof SubgraphComponent) {
					MVGraph mvSubgraph = new MVEngineGraph(((SubgraphComponent) component).getSubgraphNoMetadataPropagation());
					mvSubgraphs.put(getMVComponent(component.getId()), mvSubgraph);
				}
			}
		}
		return mvSubgraphs;
	}

	@Override
	public MVEdge getMVEdge(String edgeId) {
		Edge edge = graph.getEdges().get(edgeId);
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
	public MVEdge getMVEdgeRecursive(Edge edge) {
		if (edge.getGraph() == graph) {
			return getMVEdge(edge.getId());
		} else {
			for (MVGraph mvSubgraph : getMVSubgraphs().values()) {
				MVEdge mvEdge = mvSubgraph.getMVEdgeRecursive(edge);
				if (mvEdge != null) {
					return mvEdge;
				}
			}
			return null;
		}
	}
	
	@Override
	public MVMetadata createMVMetadata(DataRecordMetadata metadata) {
		return new MVEngineMetadata(metadata);
	}

	@Override
	public MVMetadata createMVMetadata(DataRecordMetadata metadata, int priority) {
		return new MVEngineMetadata(metadata, priority);
	}

}
