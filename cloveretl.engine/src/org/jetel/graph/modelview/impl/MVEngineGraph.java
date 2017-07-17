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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.SubgraphComponent;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVGraph;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.MVSubgraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * General model wrapper for engine transformation graph ({@link TransformationGraph}).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7. 3. 2014
 */
public class MVEngineGraph extends MVEngineGraphElement implements MVGraph {

	private static final long serialVersionUID = 5591555511415409971L;

	private Map<String, MVComponent> mvComponents;

	private Map<String, MVEdge> mvEdges;

	private Map<String, MVSubgraph> mvSubgraphs;

	public MVEngineGraph(TransformationGraph graph) {
		this(graph, null);
	}
	
	protected MVEngineGraph(TransformationGraph graph, MVComponent parentMVSubgraphComponent) {
		super(graph, parentMVSubgraphComponent);
		
		//initialize inverted edge metadata references, see MVEdge#getMetadataRefInverted()
		initMetadataReferences();
	}
	
	private void initMetadataReferences() {
		for (MVEdge edge : getMVEdges().values()) {
			MVEngineEdge metadataRef = (MVEngineEdge) edge.getMetadataRef();
			if (metadataRef != null) {
				metadataRef.addMetadataRefInverted(edge);
			}
		}
	}
	
	@Override
	public TransformationGraph getModel() {
		return (TransformationGraph) super.getModel();
	}
	
	@Override
	public void reset() {
		//reset all components
		for (MVComponent mvComponent : getMVComponents().values()) {
			mvComponent.reset();
		}
		//reset all edges
		for (MVEdge mvEdge : getMVEdges().values()) {
			mvEdge.reset();
		}
		//reset all subgraphs
		for (MVSubgraph subgraph : getMVSubgraphs().values()) {
			subgraph.reset();
		}
	}

	@Override
	public Map<String, MVComponent> getMVComponents() {
		if (mvComponents == null) {
			mvComponents = new HashMap<>();
			for (Node component : getModel().getNodes().values()) {
				mvComponents.put(component.getId(), new MVEngineComponent(component, this));
			}
		}
		return mvComponents;
	}
	
	@Override
	public MVComponent getMVComponent(String componentId) {
		Map<String, MVComponent> mvComponents = getMVComponents();
		if (mvComponents.containsKey(componentId)) {
			return mvComponents.get(componentId);
		} else {
			throw new IllegalArgumentException("unexpected component id, component does not exist in this graph");
		}
	}

	@Override
	public Map<String, MVSubgraph> getMVSubgraphs() {
		if (mvSubgraphs == null) {
			mvSubgraphs = new HashMap<>();
			for (MVComponent component : getMVComponents().values()) {
				if (component.isSubgraphComponent()) {
					TransformationGraph engineSubgraph = ((SubgraphComponent) component.getModel()).getSubgraphNoMetadataPropagation(false);
					if (engineSubgraph != null) { //can be null if the subgraph component is not correctly defined - invalid subgraph URL 
						MVSubgraph mvSubgraph = new MVEngineSubgraph(engineSubgraph, component);
						mvSubgraphs.put(component.getId(), mvSubgraph);
					}
				}
			}
		}
		return mvSubgraphs;
	}

	@Override
	public boolean hasMVEdge(String edgeId) {
		Map<String, MVEdge> mvEdges = getMVEdges();
		return mvEdges.containsKey(edgeId);
	}

	@Override
	public MVEdge getMVEdge(String edgeId) {
		Map<String, MVEdge> mvEdges = getMVEdges();
		
		MVEdge result = mvEdges.get(edgeId);
		if (result == null) {
			throw new IllegalArgumentException("unexpected edge id, edge does not exist in this graph");
		}
		return result;
	}
	
	@Override
	public MVEdge getMVEdgeRecursive(Edge engineEdge) {
		if (engineEdge.getGraph() == getModel()) {
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
		if (getModel() == engineGraph) {
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
		if (mvEdges == null) {
			mvEdges = new HashMap<>();
			for (Edge edge : getModel().getEdges().values()) {
				mvEdges.put(edge.getId(), new MVEngineEdge(edge, this));
			}
		}
		return mvEdges;
	}
	
	@Override
	public List<MVEdge> getMVEdgesRecursive() {
		List<MVEdge> result = new ArrayList<>();
		result.addAll(getMVEdges().values());
		for (MVGraph subgraph : getMVSubgraphs().values()) {
			result.addAll(subgraph.getMVEdgesRecursive());
		}
		return result;
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
	public MVComponent getParent() {
		return (MVComponent) super.getParent();
	}
	
}
