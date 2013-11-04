/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.util;

import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Edge;
import org.jetel.graph.EdgeFactory;
import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;

/**
 * This is utility class for graph manipulation.  
 * 
 * The code should be moved to proper place in the future.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.12.2012
 * @see TransformationAnalyser
 * @see ClusteredGraphProvider
 */
public class GraphUtils {

	/**
	 * Inserts the given component into the given edge.
	 */
	public static void insertComponent(Node insertedComponent, Edge edge) {
		TransformationGraph graph = edge.getGraph();
		
		//insert component into correct phase
		Phase phase = edge.getWriter().getPhase();
		phase.addNode(insertedComponent);
		
		//create the left artificial edge
		Edge leftEdge = EdgeFactory.newEdge(edge.getId() + "_inserted", edge);
		Node writer = edge.getWriter();
		Node reader = insertedComponent;
		writer.addOutputPort(edge.getOutputPortNumber(), leftEdge);
		reader.addInputPort(0, leftEdge);
		try {
			graph.addEdge(leftEdge);
		} catch (GraphConfigurationException e) {
			throw new JetelRuntimeException("Component '" + insertedComponent + "' cannot be inserted into graph.", e);
		}
		
		//re-attach the edge
		writer = insertedComponent;
		reader = edge.getReader();
		writer.addOutputPort(0, edge);
		reader.addInputPort(edge.getInputPortNumber(), edge);
	}
	
	/**
	 * Removes the component from graph. Component has
	 * to have equal number of input and output edges.
	 * These edges are re-connected in 'pass-through' way.
	 * @param component component to be removed from graph
	 */
	public static void removeComponent(Node component) {
		if (component.getInPorts().size() == component.getOutPorts().size()) {
			TransformationGraph graph = component.getGraph();
			
			for (int i = 0; i < component.getInPorts().size(); i++) {
				Edge leftEdge = (Edge) component.getInputPort(i);
				Edge rightEdge = (Edge) component.getOutputPort(i);
				Node rightComponent = rightEdge.getReader();
				
				//remove the right edge
				try {
					graph.deleteEdge(rightEdge);
				} catch (GraphConfigurationException e) {
					throw new JetelRuntimeException("Component '" + component + "' cannot be removed from graph.", e);
				}
				
				//re-attach the left edge
				rightComponent.addInputPort(rightEdge.getInputPortNumber(), leftEdge);
			}

			//remove component from phase
			component.getPhase().deleteNode(component);
		} else {
			throw new JetelRuntimeException("Component '" + component + "' cannot be removed from graph. Number of input edges is not equal to number of output edges.");
		}
	}
	

	/**
	 * The graph duplicate is not valid graph, only basic structure of the graph
	 * is duplicated. The duplicate is used for graph cycle detection in clustered graphs
	 * and few other places.
	 * @param templateGraph graph which is duplicated
	 * @return structural copy of the given graph
	 */
	public static TransformationGraph duplicateGraph(TransformationGraph templateGraph) {
		TransformationGraph graph = new TransformationGraph(templateGraph.getId());
		graph.setJobType(templateGraph.getJobType());
		
		try {
			for (Phase templatePhase : templateGraph.getPhases()) {
				duplicatePhase(graph, templatePhase);
			}

			for (Edge templateEdge : templateGraph.getEdges().values()) {
				duplicateEdge(graph, templateEdge);
			}
		} catch (GraphConfigurationException e) {
			throw new JetelRuntimeException("Graph cannot be duplicated.", e);
		}
		
		return graph;
	}

	/**
	 * @param graph
	 * @param templatePhase
	 * @throws GraphConfigurationException 
	 */
	private static void duplicatePhase(TransformationGraph graph, Phase templatePhase) throws GraphConfigurationException {
		Phase phase = new Phase(templatePhase.getPhaseNum());
		graph.addPhase(phase);
		for (Node templateComponent : templatePhase.getNodes().values()) {
			duplicateComponent(phase, templateComponent);
		}
	}

	/**
	 * @param phase
	 * @param templateEdge
	 * @throws GraphConfigurationException 
	 */
	private static void duplicateEdge(TransformationGraph graph, Edge templateEdge) throws GraphConfigurationException {
		Edge edge = EdgeFactory.newEdge(templateEdge.getId(), templateEdge);
		Node writer = graph.getNodes().get(templateEdge.getWriter().getId());
		Node reader = graph.getNodes().get(templateEdge.getReader().getId());
		writer.addOutputPort(templateEdge.getOutputPortNumber(), edge);
		reader.addInputPort(templateEdge.getInputPortNumber(), edge);
		graph.addEdge(edge);
	}

	/**
	 * @param templateComponent
	 * @return
	 */
	private static void duplicateComponent(Phase phase, Node templateComponent) {
		ComponentMockup component = new ComponentMockup(templateComponent.getId(), templateComponent.getType());
		component.setName(templateComponent.getName());
		component.setEnabled(templateComponent.getEnabled());
		component.setAllocation(templateComponent.getAllocation().duplicate());
		component.setUsedUrls(templateComponent.getUsedUrls());
		phase.addNode(component);
	}
	
	private static class ComponentMockup extends Node {
		private String type;
		private String[] usedUrls;
		public ComponentMockup(String id, String type) {
			super(id);
			this.type = type;
		}
		
		@Override
		public String getType() {
			return type;
		}

		@Override
		protected Result execute() throws Exception {
			return null;
		}
		
		public void setUsedUrls(String[] usedUrls) {
			this.usedUrls = usedUrls;
		}
		
		@Override
		public String[] getUsedUrls() {
			return usedUrls;
		}
	}

	/**
	 * Finds unique identifier for a component in the given graph.
	 * The identifier is derived from the suggestedId parameter.
	 * @param graph
	 * @param clusterRegatherType
	 * @return
	 */
	public static String getUniqueComponentId(TransformationGraph graph, String suggestedId) {
		if (isUniqueComponentId(graph, suggestedId)) {
			return suggestedId;
		}
		
		int i = 1;
		String newSuggestedId = null;
		do {
			newSuggestedId = suggestedId + (i++);
		} while(!isUniqueComponentId(graph, newSuggestedId));
		return newSuggestedId;
	}

	private static boolean isUniqueComponentId(TransformationGraph graph, String suggestedId) {
		for (Node component : graph.getNodes().values()) {
			if (component.getId().equals(suggestedId)) {
				return false;
			}
		}
		return true;
	}

	
	private static EdgeTypeEnum[][] edgeCombinations;

	private static EdgeTypeEnum[][] getEdgeCombinations() {
		if (edgeCombinations == null) {
			 edgeCombinations = new EdgeTypeEnum[EdgeTypeEnum.values().length][EdgeTypeEnum.values().length];

			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.DIRECT;
			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.BUFFERED;
			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.DIRECT_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;

			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.BUFFERED;
			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.BUFFERED;
			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;

			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;

			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.DIRECT_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.DIRECT_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;

			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
		}
		return edgeCombinations;
	}

	/**
	 * This method derives from two edge types an edge type which should satisfy needs from both.
	 */
	public static EdgeTypeEnum combineEdges(EdgeTypeEnum edgeType1, EdgeTypeEnum edgeType2) {
		return getEdgeCombinations()[edgeType1.ordinal()][edgeType2.ordinal()];
	}
	
}
