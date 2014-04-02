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
package org.jetel.graph;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.enums.EnabledEnum;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.analyse.GraphCycleInspector;
import org.jetel.graph.analyse.SingleGraphProvider;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVGraph;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MVEngineGraph;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.SingleThreadWatchDog;
import org.jetel.util.SubgraphUtils;

/*
 *  import org.apache.log4j.Logger;
 *  import org.apache.log4j.BasicConfigurator;
 */
/**
 * A class that analyzes relations between Nodes and Edges of the Transformation Graph
 * 
 * @author D.Pavlis
 * @since April 2, 2002
 * @see OtherClasses
 */

public class TransformationGraphAnalyzer {

	static Log logger = LogFactory.getLog(TransformationGraphAnalyzer.class);

	static PrintStream log = System.out;// default info messages to stdout

	/**
	 * Several pre-execution steps is performed in this graph analysis.
	 * - disable nodes are removed from graph
	 * - subgraph related updates are performed
	 * - automatic metadata propagation is performed
	 * - correct edge types are detected
	 */
	public static void analyseGraph(TransformationGraph graph, GraphRuntimeContext runtimeContext, boolean propagateMetadata){
        //remove disabled components and their edges
		try {
			TransformationGraphAnalyzer.disableNodesInPhases(graph);
		} catch (GraphConfigurationException e) {
			throw new JetelRuntimeException("Removing disabled nodes failed.", e);
		}

		boolean subJobRuntime = runtimeContext.getJobType().isSubJob();
		boolean subJobFile = runtimeContext.getJobType().isSubJob() || graph.getStaticJobType().isSubJob();
		if (subJobRuntime || subJobFile) {
			try {
				boolean removeDebugNodes = subJobRuntime;
				boolean layoutChecking = subJobFile;
				TransformationGraphAnalyzer.analyseSubgraph(graph, removeDebugNodes, layoutChecking);
			} catch (Exception e) {
				throw new JetelRuntimeException("Subgraph analysis failed.", e);
			}
		}
		
		//perform automatic metadata propagation
		if (propagateMetadata) {
			//create model view for the graph
			MVGraph mvGraph = new MVEngineGraph(graph, null);
			//first analyse subgraphs calling hierarchy - cannot be recursive
			TransformationGraphAnalyzer.analyseSubgraphCallingHierarchy(mvGraph);
			try {
				TransformationGraphAnalyzer.analyseMetadataPropagation(mvGraph);
			} catch (Exception e) {
				throw new JetelRuntimeException("Metadata propagation analysis failed.", e);
			}
		}

        //analyze type of edges - specially buffered and phase edges
        try {
        	TransformationGraphAnalyzer.analyseEdgeTypes(graph);
		} catch (Exception e) {
			throw new JetelRuntimeException("Edge type analysis failed.", e);
		}
        
        graph.setAnalysed(true);
	}
	
	/**
	 * Check whether subgraph calling hierarchy of the given graph is not recursive.
	 * @param graph
	 */
	private static void analyseSubgraphCallingHierarchy(MVGraph graph) {
		analyseSubgraphCallingHierarchy(graph, null, new ArrayList<String>());
	}
	
	private static void analyseSubgraphCallingHierarchy(MVGraph graph, MVComponent causedComponent, List<String> urlStack) {
		boolean topLevel = urlStack.isEmpty();
		String url = graph.getModel().getRuntimeContext().getJobUrl();
		if (urlStack.contains(url)) {
			throw new JetelRuntimeException("Recursive subgraph hierarchy detected in " + url);
		} else {
			urlStack.add(url);
		}
		for (Entry<MVComponent, MVGraph> subgraph : graph.getMVSubgraphs().entrySet()) {
			if (topLevel) {
				causedComponent = subgraph.getKey();
			}
			analyseSubgraphCallingHierarchy(subgraph.getValue(), causedComponent, urlStack);
		}
		urlStack.remove(url);
	}

	/**
	 * Performs automatic metadata propagation on the given graph.
	 */
	private static void analyseMetadataPropagation(MVGraph mvGraph) {
		//craete metatadata propagation resolver
		MetadataPropagationResolver metadataPropagationResolver = new MetadataPropagationResolver(mvGraph);
		//analyse the graph
		metadataPropagationResolver.analyseGraph();
		//copy propagated metadata into transformation graph
		for (Edge edge : mvGraph.getModel().getEdges().values()) {
			MVMetadata metadata = metadataPropagationResolver.getMVEdge(edge).getMetadata();
			if (metadata != null) {
				edge.setMetadata(metadata.getModel());
			}
		}
		//store complete resolver into graph for further usage (mainly in designer)
		mvGraph.getModel().setMetadataPropagationResolver(metadataPropagationResolver);
	}

	private static final class SubgraphAnalyzer {
		private final TransformationGraph subgraph;

		public SubgraphAnalyzer(TransformationGraph subgraph) {
			this.subgraph = subgraph;
		}
		
		public SubgraphAnalysisResult analyzeForRuntime() {
			return analyze(false);
		}
		
		public SubgraphAnalysisResult analyzeForValidation() {
			return analyze(true);
		}
		
		private SubgraphAnalysisResult analyze(boolean layoutChecking) {
			SubgraphAnalysisResult result = new SubgraphAnalysisResult();
			
			result.setSubgraph(subgraph);
			Collection<Node> nodes = subgraph.getNodes().values();
			for (Node component : nodes) {
				if (SubgraphUtils.isSubJobInputComponent(component.getType())) {
					result.setSubgraphInput(component);
					result.setSubgraphInputPrecedentNodes(TransformationGraphAnalyzer.findPrecedentNodesRecursive(component, null));
					if (layoutChecking) {
						result.setSubgraphInputFollowingNodes(TransformationGraphAnalyzer.findFollowingNodesRecursive(component, null));
					}
				}
				if (SubgraphUtils.isSubJobOutputComponent(component.getType())) {
					result.setSubgraphOutput(component);
					result.setSubgraphOutputFollowingNodes(TransformationGraphAnalyzer.findFollowingNodesRecursive(component, null));
					if (layoutChecking) {
						result.setSubgraphOutputPrecedentNodes(TransformationGraphAnalyzer.findPrecedentNodesRecursive(component, null));
					}
				}
			}
			
			sortComponents(result, nodes);
			
			return result;
		}

		private void sortComponents(SubgraphAnalysisResult result, Collection<Node> nodes) {
			Set<Node> debugInputNodes = new HashSet<>();
			Set<Node> debugOutputNodes = new HashSet<>();
			Set<Node> activeNodes = new HashSet<>();
			if (result.getSubgraphInputPrecedentNodes() != null) {
				debugInputNodes.addAll(result.getSubgraphInputPrecedentNodes());
			}
			if (result.getSubgraphOutputFollowingNodes() != null) {
				debugOutputNodes.addAll(result.getSubgraphOutputFollowingNodes());
			}
			int beginPhase = result.getSubgraphInput() != null ? result.getSubgraphInput().getPhaseNum() : Integer.MIN_VALUE;
			int endPhase = result.getSubgraphOutput() != null ? result.getSubgraphOutput().getPhaseNum() : Integer.MAX_VALUE;
			for (Node component : nodes) {
				if (component == result.getSubgraphInput() || component == result.getSubgraphOutput()) {
					continue;
				}
				if (component.getPhaseNum() < beginPhase && !debugInputNodes.contains(component)) {
					debugInputNodes.add(component);
				}
				else if (component.getPhaseNum() > endPhase && !debugOutputNodes.contains(component)) {
					debugOutputNodes.add(component);
				}
				else {
					activeNodes.add(component);
				}
			}
			result.setDebugInputNodes(debugInputNodes);
			result.setDebugOutputNodes(debugOutputNodes);
			result.setActiveNodes(activeNodes);
		}
	}
	
	private static class SubgraphAnalysisValidationException extends Exception {
		private static final long serialVersionUID = -8502212649423859074L;

		public SubgraphAnalysisValidationException(String message) {
			super(message);
		}
	}
	
	private static class SubgraphAnalysisResult {
		private TransformationGraph subgraph;
		private Node subgraphInput;
		private Node subgraphOutput;
		
		private List<Node> subgraphInputPrecedentNodes;
		private List<Node> subgraphInputFollowingNodes;
		
		private List<Node> subgraphOutputPrecedentNodes;
		private List<Node> subgraphOutputFollowingNodes;
		private Set<Node> debugInputNodes;
		private Set<Node> debugOutputNodes;
		private Set<Node> activeNodes;
		
		public TransformationGraph getSubgraph() {
			return subgraph;
		}
		public void setSubgraph(TransformationGraph subgraph) {
			this.subgraph = subgraph;
		}
		public Node getSubgraphInput() {
			return subgraphInput;
		}
		public Set<Node> getActiveNodes() {
			return activeNodes;
		}
		public void setActiveNodes(Set<Node> activeNodes) {
			this.activeNodes = activeNodes;
		}
		public Set<Node> getDebugInputNodes() {
			return debugInputNodes;
		}
		public void setDebugInputNodes(Set<Node> debugInputNodes) {
			this.debugInputNodes = debugInputNodes;
		}
		public Set<Node> getDebugOutputNodes() {
			return debugOutputNodes;
		}
		public void setDebugOutputNodes(Set<Node> debugOutputNodes) {
			this.debugOutputNodes = debugOutputNodes;
		}
		public void setSubgraphInput(Node subgraphInput) {
			this.subgraphInput = subgraphInput;
		}
		public Node getSubgraphOutput() {
			return subgraphOutput;
		}
		public void setSubgraphOutput(Node subgraphOutput) {
			this.subgraphOutput = subgraphOutput;
		}
		public List<Node> getSubgraphInputPrecedentNodes() {
			return subgraphInputPrecedentNodes;
		}
		public void setSubgraphInputPrecedentNodes(List<Node> subgraphInputPrecedentNodes) {
			this.subgraphInputPrecedentNodes = subgraphInputPrecedentNodes;
		}
		public List<Node> getSubgraphInputFollowingNodes() {
			return subgraphInputFollowingNodes;
		}
		public void setSubgraphInputFollowingNodes(List<Node> subgraphInputFollowingNodes) {
			this.subgraphInputFollowingNodes = subgraphInputFollowingNodes;
		}
		public List<Node> getSubgraphOutputPrecedentNodes() {
			return subgraphOutputPrecedentNodes;
		}
		public void setSubgraphOutputPrecedentNodes(List<Node> subgraphOutputPrecedentNodes) {
			this.subgraphOutputPrecedentNodes = subgraphOutputPrecedentNodes;
		}
		public List<Node> getSubgraphOutputFollowingNodes() {
			return subgraphOutputFollowingNodes;
		}
		public void setSubgraphOutputFollowingNodes(List<Node> subgraphOutputFollowingNodes) {
			this.subgraphOutputFollowingNodes = subgraphOutputFollowingNodes;
		}
		public void validate() throws SubgraphAnalysisValidationException {
			if (subgraphInputFollowingNodes != null && subgraphInputPrecedentNodes != null) {
				if (!CollectionUtils.intersection(subgraphInputFollowingNodes, subgraphInputPrecedentNodes).isEmpty()) {
					throw new SubgraphAnalysisValidationException("A component preceding the SubgraphInput component is probably connected with a component following SubgraphInput.");
				}
			}
			if (subgraphOutputFollowingNodes != null && subgraphOutputPrecedentNodes != null) {
				if (!CollectionUtils.intersection(subgraphOutputFollowingNodes, subgraphOutputPrecedentNodes).isEmpty()) {
					throw new SubgraphAnalysisValidationException("A component following the SubgraphOutput component is probably connected with a component preceding SubgraphOutput.");
				}
			}
			if (subgraphInputPrecedentNodes != null && subgraphOutput != null && subgraphInputPrecedentNodes.contains(subgraphOutput)) {
				throw new SubgraphAnalysisValidationException("SubgraphOutput is connected to a component preceding SubgraphInput");
			}
			if (subgraphOutputFollowingNodes != null && subgraphInput != null && subgraphOutputFollowingNodes.contains(subgraphInput)) {
				throw new SubgraphAnalysisValidationException("SubgraphInput is connected to a component following SubgraphOutput");
			}
			for (Edge edge : getSubgraph().getEdges().values()) {
				Node reader = edge.getReader();
				Node writer = edge.getWriter();
				
				if (reader == subgraphInput || reader == subgraphOutput || writer == subgraphInput || writer == subgraphOutput) {
					continue;
				}
				
				if (reader != null && writer != null) {
					validateEdge(reader, writer);
					validateEdge(writer, reader);
				}
			}
			if (subgraphInput != null) {
				for (Node node : subgraphInputPrecedentNodes) {
					if (node.getPhaseNum() > subgraphInput.getPhaseNum()) {
						throw new SubgraphAnalysisValidationException(String.format("Component %s is connected to debugging input components but is in phase %d", node.getName(), node.getPhaseNum()));
					}
				}
			}
			if (subgraphOutput != null) {
				for (Node node : subgraphOutputFollowingNodes) {
					if (subgraphOutput.getPhaseNum() > node.getPhaseNum()) {
						throw new SubgraphAnalysisValidationException(String.format("Component %s is connected to debugging output components but is in phase %d", node.getName(), node.getPhaseNum()));
					}
				}
			}
		}
		private void validateEdge(Node componentA, Node componentB) throws SubgraphAnalysisValidationException {
			if (debugInputNodes.contains(componentA) && !debugInputNodes.contains(componentB)) {
				throw new SubgraphAnalysisValidationException(String.format("Debugging input component %s is connected to %s which is not a debugging input component.",
						componentA.getName(), componentB.getName()));
			}
			if (debugOutputNodes.contains(componentA) && !debugOutputNodes.contains(componentB)) {
				throw new SubgraphAnalysisValidationException(String.format("Debugging output component %s is connected to %s which is not a debugging output component.",
						componentA.getName(), componentB.getName()));
			}
		}
		public Integer getStartPhase() {
			return subgraphInput != null ? subgraphInput.getPhaseNum() : null;
		}
		public Integer getEndPhase() {
			return subgraphOutput != null ? subgraphOutput.getPhaseNum() : null;
		}
		public boolean isActivePhase(int phaseNum) {
			if (getStartPhase() != null && phaseNum < getStartPhase()) {
				return false;
			}
			if (getEndPhase() != null && getEndPhase() < phaseNum) {
				return false;
			}
			return true;
		}
	}
	
	/**
	 * Checks subgraph layout and removes all components before SubgraphInput and after SubgraphOutput.
	 */
	public static void analyseSubgraph(TransformationGraph graph, boolean removeDebugNodes, boolean layoutChecking) {
		SubgraphAnalyzer analyzer = new SubgraphAnalyzer(graph);
		SubgraphAnalysisResult analysisResult = layoutChecking ? analyzer.analyzeForValidation() : analyzer.analyzeForRuntime();
		if (layoutChecking) {
			try {
				analysisResult.validate();
			} catch (SubgraphAnalysisValidationException e) {
				throw new JetelRuntimeException("Invalid subgraph layout.", e);
			}
		}
		
		if (removeDebugNodes) {
			removeDebugNodes(graph, analysisResult);
		}
	}

	private static void removeDebugNodes(TransformationGraph graph, SubgraphAnalysisResult analysisResult) {
		for (Node node : analysisResult.getDebugInputNodes()) {
			node.setEnabled(EnabledEnum.DISABLED);
		}
		for (Node node : analysisResult.getDebugOutputNodes()) {
			node.setEnabled(EnabledEnum.DISABLED);
		}

		try {
			TransformationGraphAnalyzer.disableNodesInPhases(graph);
		} catch (GraphConfigurationException e) {
			throw new JetelRuntimeException("Failed to remove disabled/pass-through nodes from subgraph.", e);
		}
	}

	/**
	 * Detects suitable type of edges for the given graph. Edge types are preset
	 * directly to the graph instance.
	 */
	public static void analyseEdgeTypes(TransformationGraph graph) {
		//first of all find the phase edges
		analysePhaseEdges(graph);

		//let's find cycles of relationships in the graph and interrupted them by buffered edges to avoid deadlocks
		GraphCycleInspector graphCycleInspector = new GraphCycleInspector(new SingleGraphProvider(graph));
		graphCycleInspector.inspectGraph();
		
		//update edge types around Subgraph components
		//real edge is combination of parent graph edge type and subgraph edge type
		//this is turned off - parent graph is not changed according child graph, at least for now
//		for (Node component : graph.getNodes().values()) {
//			if (component instanceof SubgraphComponent) {
//				SubgraphComponent subgraphComponent = (SubgraphComponent) component;
//				for (Entry<Integer, InputPort> inputPort : component.getInputPorts().entrySet()) {
//					Edge subgraphEdge = subgraphComponent.getSubgraphInputEdge(inputPort.getKey());
//					Edge parentGraphEdge = inputPort.getValue().getEdge();
//					//will be edge base shared between these two edges?
//					if (SubgraphUtils.isSubgraphInputEdgeShared(subgraphEdge, parentGraphEdge)) {
//						//so we need to combine both edge types to satisfy needs of both parent and subgraph
//						EdgeTypeEnum combinedEdgeType = GraphUtils.combineEdges(parentGraphEdge.getEdgeType(), subgraphEdge.getEdgeType());
//						parentGraphEdge.setEdgeType(combinedEdgeType);
//					}
//				}
//				for (Entry<Integer, OutputPort> outputPort : component.getOutputPorts().entrySet()) {
//					Edge subgraphEdge = subgraphComponent.getSubgraphOutputEdge(outputPort.getKey());
//					Edge parentGraphEdge = outputPort.getValue().getEdge();
//					//will be edge base shared between these two edges?
//					if (SubgraphUtils.isSubgraphOutputEdgeShared(subgraphEdge, parentGraphEdge)) {
//						//so we need to combine both edge types to satisfy needs of both parent and subgraph
//						EdgeTypeEnum combinedEdgeType = GraphUtils.combineEdges(parentGraphEdge.getEdgeType(), subgraphEdge.getEdgeType());
//						parentGraphEdge.setEdgeType(combinedEdgeType);
//					}
//				}
//			}
//		}
	}

	private static void analysePhaseEdges(TransformationGraph graph) {
		Phase readerPhase;
		Phase writerPhase;

		// analyse edges (whether they need to be buffered and put them into proper phases
		// edges connecting nodes from two different phases has to be put into both phases
		for (Edge edge : graph.getEdges().values()) {
			Node reader = edge.getReader(); //can be null for remote edges
			Node writer = edge.getWriter(); //can be null for remote edges
			readerPhase = reader != null ? reader.getPhase() : null;
			writerPhase = writer != null ? writer.getPhase() : null;
			if (readerPhase.getPhaseNum() > writerPhase.getPhaseNum()) {
				// edge connecting two nodes belonging to different phases
				// has to be buffered
				edge.setEdgeType(EdgeTypeEnum.PHASE_CONNECTION);
			}
		}
	}

	/**
	 * Apply disabled property of node to graph. Called in graph initial phase.
	 * 
	 * @throws GraphConfigurationException
	 */
	public static void disableNodesInPhases(TransformationGraph graph) throws GraphConfigurationException {
		Set<Node> nodesToRemove = new HashSet<Node>();
		Phase[] phases = graph.getPhases();

		for (int i = 0; i < phases.length; i++) {
			nodesToRemove.clear();
			for (Node node : phases[i].getNodes().values()) {
				if (node.getEnabled() == EnabledEnum.DISABLED) {
					nodesToRemove.add(node);
					disconnectAllEdges(node);
				} else if (node.getEnabled() == EnabledEnum.PASS_THROUGH) {
					nodesToRemove.add(node);
					final InputPort inputPort = node.getInputPort(node.getPassThroughInputPort());
					final OutputPort outputPort = node.getOutputPort(node.getPassThroughOutputPort());
					if (inputPort == null || outputPort == null
					// if the component has an output edge which is directly connected into its input port
					// whole component is removed even with the edge
					// this is not normally possible however see issue #4960
					|| inputPort.getEdge() == outputPort.getEdge()) {
						disconnectAllEdges(node);
						continue;
					}
					final Edge inEdge = inputPort.getEdge();
					final Edge outEdge = outputPort.getEdge();
					final Node sourceNode = inEdge.getWriter();
					final Node targetNode = outEdge.getReader();
					final int sourceIdx = inEdge.getOutputPortNumber();
					final int targetIdx = outEdge.getInputPortNumber();
					disconnectAllEdges(node);
					sourceNode.addOutputPort(sourceIdx, inEdge);
					targetNode.addInputPort(targetIdx, inEdge);
					try {
						node.getGraph().addEdge(inEdge);
					} catch (GraphConfigurationException e) {
						logger.error(e);
					}
				}
			}
			for (Node node : nodesToRemove) {
				phases[i].deleteNode(node);
			}
		}
	}

	/**
	 * Disconnect all edges connected to the given node.
	 * 
	 * @param node
	 * @throws GraphConfigurationException
	 */
	private static void disconnectAllEdges(Node node) throws GraphConfigurationException {
		for (Iterator<InputPort> it1 = node.getInPorts().iterator(); it1.hasNext();) {
			final Edge edge = it1.next().getEdge();
			Node writer = edge.getWriter();
			if (writer != null)
				writer.removeOutputPort(edge);
			node.getGraph().deleteEdge(edge);
		}

		for (Iterator<OutputPort> it1 = node.getOutPorts().iterator(); it1.hasNext();) {
			final Edge edge = it1.next().getEdge();
			final Node reader = edge.getReader();
			if (reader != null)
				reader.removeInputPort(edge);
			node.getGraph().deleteEdge(edge);
		}
	}

	/**
	 * @param node
	 * @param reflectedNodes
	 *            reflected set of nodes, typically nodes in phase; the resulted nodes will be only from this set of
	 *            nodes
	 * @return list of all precedent nodes for given node
	 */
	public static List<Node> findPrecedentNodes(Node node, Collection<Node> reflectedNodes) {
		List<Node> result = new ArrayList<Node>();

		for (InputPort inputPort : node.getInPorts()) {
			final Node writer = inputPort.getWriter();
			if (reflectedNodes == null || reflectedNodes.contains(writer)) {
				result.add(writer);
			}
		}

		return result;
	}

	/**
	 * Finds all components which precede given root component. Recursive version of {@link #findPrecedentNodes(Node, Collection)} method.
	 */
	public static List<Node> findPrecedentNodesRecursive(Node rootComponent, Collection<Node> reflectedNodes) {
		List<Node> result = new ArrayList<Node>();
		Queue<Node> toProcess = new LinkedList<Node>();
		
		toProcess.addAll(findPrecedentNodes(rootComponent, reflectedNodes));
		while (!toProcess.isEmpty()) {
			Node component = toProcess.poll();
			if (!result.contains(component) && component != rootComponent) {
				toProcess.addAll(findPrecedentNodes(component, reflectedNodes));
				toProcess.addAll(findFollowingNodes(component, reflectedNodes));
				result.add(component);
			}
		}
		
		return result;
	}
	
	/**
	 * @param node
	 * @param reflectedNodes
	 *            reflected set of nodes, typically nodes in phase; the resulted nodes will be only from this set of
	 *            nodes
	 * @return list of all following nodes for given node
	 */
	public static List<Node> findFollowingNodes(Node node, Collection<Node> reflectedNodes) {
		List<Node> result = new ArrayList<Node>();

		for (OutputPort outputPort : node.getOutPorts()) {
			final Node reader = outputPort.getReader();
			if (reflectedNodes == null || reflectedNodes.contains(reader)) {
				result.add(reader);
			}
		}

		return result;
	}

	/**
	 * Finds all components which follow given root component. Recursive version of {@link #findFollowingNodes(Node, Collection)} method.
	 */
	public static List<Node> findFollowingNodesRecursive(Node rootComponent, Collection<Node> reflectedNodes) {
		List<Node> result = new ArrayList<Node>();
		Queue<Node> toProcess = new LinkedList<Node>();
		
		toProcess.addAll(findFollowingNodes(rootComponent, reflectedNodes));
		while (!toProcess.isEmpty()) {
			Node component = toProcess.poll();
			if (!result.contains(component) && component != rootComponent) {
				toProcess.addAll(findPrecedentNodes(component, reflectedNodes));
				toProcess.addAll(findFollowingNodes(component, reflectedNodes));
				result.add(component);
			}
		}
		
		return result;
	}

	/**
	 * Components topological sorting based on Kahn algorithm.
	 * This algorithm is used for user friendly nodes visualisation
	 * and for single thread graph execution, see {@link SingleThreadWatchDog}.
	 * 
	 * @param givenNodes scope in which the topological sorting is performed - only mentioned components are considered
	 * @return given nodes in topological order 
	 * @note algorithm is described for example at http://en.wikipedia.org/wiki/Topological_sorting
	 */
	public static List<Node> nodesTopologicalSorting(List<Node> givenNodes) {
		List<Node> result = new ArrayList<Node>();
		Stack<Node> roots = new Stack<Node>();
		List<Node> loopComponents = new ArrayList<Node>(); 
		
		// find root nodes - nodes without precedent nodes in the given list of nodes
		for (Node givenNode : givenNodes) {
			if (findPrecedentNodes(givenNode, givenNodes).isEmpty()) {
				roots.add(givenNode);
			} else {
				if (givenNode.getType().equals(GraphCycleInspector.LOOP_COMPONENT_TYPE)) {
					loopComponents.add(givenNode);
				}
			}
		}

		//start with topological sorting using the roots
		processRoots(givenNodes, roots, result);
		
		//if some components are no in the result - an oriented cycle has been found
		//oriented cycle is now supported only for Loop component, which is natural root of these cycles
		//let's start with topological sorting with Loop components as roots
		if (result.size() < givenNodes.size()) {
			//find all Loop components which are not in result
			for (Node loopComponent : loopComponents) {
				if (!result.contains(loopComponent)) {
					roots.add(loopComponent);
				}
			}
			
			processRoots(givenNodes, roots, result);
		}

		//seems that the graph contains some oriented cycles without Loop component
		//so take one component by one and use them as root for sorting iterations
		while (result.size() < givenNodes.size()) {
			for (Node givenNode : givenNodes) {
				if (!result.contains(givenNode)) {
					roots.add(givenNode);
					processRoots(givenNodes, roots, result);
					break;
				}
			}
		}

		return result;
	}

	private static void processRoots(List<Node> givenNodes, Stack<Node> roots, List<Node> result) {
		// topological sorting
		while (!roots.isEmpty()) {
			Node root = roots.pop();
			if (!result.contains(root)) {
				result.add(root);
				List<OutputPort> outputPorts = new ArrayList<OutputPort>(root.getOutPorts());
				//let's reverse the output ports to get more logical output
				//Loop component is only exception where reversing is not desired
				if (!root.getType().equals(GraphCycleInspector.LOOP_COMPONENT_TYPE)) {
					Collections.reverse(outputPorts);
				}
				
				for (OutputPort outputPort : outputPorts) {
					Node followingComponent = outputPort.getReader();
					if (givenNodes.contains(followingComponent) && !result.contains(followingComponent)) {
						boolean isNewRoot = true;
						for (InputPort inputPort : followingComponent.getInPorts()) {
							if (!result.contains(inputPort.getEdge().getWriter())
									&& givenNodes.contains(inputPort.getEdge().getWriter())) {
								isNewRoot = false;
								break;
							}
						}
						if (isNewRoot) {
							roots.push(followingComponent);
						}
					}
				}
			}
		}
	}
	
}
/*
 * end class TransformationGraphAnalyzer
 */

