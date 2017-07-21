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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.jetel.component.MetadataProvider;
import org.jetel.graph.Edge;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVGraph;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.EqualsUtil;
import org.jetel.util.ReferenceState;

/**
 * General metadata propagation evaluator.
 * Metadata propagation algorithm is performed on model view (package org.jetel.graph.modelview).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 9. 2013
 */
public class MetadataPropagationResolver implements Serializable {
	
	private static final long serialVersionUID = -722143991270518507L;
	
	/** Analysed graph */
	private MVGraph mvGraph;
	
	/**
	 * Creates resolver for automatic metadata propagation on the given graph.
	 */
	public MetadataPropagationResolver(MVGraph mvGraph) {
		this.mvGraph = mvGraph;
	}
	
	/**
	 * Main trigger for metadata propagation.
	 */
	public void analyseGraph() {
		//find "no metadata" for all edges with explicit metadata
		//"no metadata" is metadata, which would be used if the edge does not have the explicit metadata associated
		if (mvGraph.getModel().getRuntimeContext().isCalculateNoMetadata()) {
			findAllNoMetadata();
		}

		//perform metadata propagation
		propagateMetadata();
	}

	/**
	 * Metadata propagation is performed on {@link #mvGraph} model.
	 */
	private void propagateMetadata() {
		//all edges in graph hierarchy (subgraphs included) will be visited
		Set<MVEdge> edgesToProcess = new HashSet<>(mvGraph.getMVEdgesRecursive());
		//affected edges are edges which will be visited in next iteration - all edges 'near' to updated edges
		Set<MVEdge> affectedEdges = new HashSet<>();
		while (!edgesToProcess.isEmpty()) { //do we have edges to visit?
			//try to improve metadata propagation for all edges with potential to be changed
			for (MVEdge mvEdge : edgesToProcess) {
				if (updateMetadata(mvEdge)) {
					//edge metadata has been updated, all 'near' edges will be added into affectedEdges and visited in next iteration
					appendRelatedEdges(mvEdge, affectedEdges);
				}
			}
			//switch edgesToProcess and affectedEdges for next iteration
			Set<MVEdge> tmp = edgesToProcess;
			edgesToProcess = affectedEdges;
			affectedEdges = tmp;
			affectedEdges.clear();
			edgesToProcess.remove(null);
		}
	}
	
	/**
	 * Tries to find better metadata propagation for the given edge.
	 * @param edge inspected edge
	 * @return true if the metadata for the given edge has been changed
	 */
	private boolean updateMetadata(MVEdge edge) {
		//metadata propagation for edges with explicit metadata cannot be improved
		//special case are edges with explicit metadata but with an implicit metadata as well - see #findAllNoMetadata()
		if (edge.hasImplicitMetadata() || !edge.hasExplicitMetadata()) {
			MVMetadata oldMetadata = edge.getMetadata();
			MVMetadata newMetadata = findMetadata(edge);
			if (!EqualsUtil.areEqual(newMetadata, oldMetadata)) {
				edge.setImplicitMetadata(newMetadata);
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Find all edges which can be directly affected by metadata updated for the given edge and
	 * append them to the affectedEdges set.
	 * The affected edges will be analysed for potential metadata propagation in next iteration.
	 */
	private void appendRelatedEdges(MVEdge edge, Set<MVEdge> affectedEdges) {
		//find affected edges for reader component
		MVComponent reader = edge.getReader();
		if (reader.isSubgraphComponent()) {
			if (reader.getSubgraph() != null) { //can be null if the subgraph reference is not valid
				affectedEdges.add(reader.getSubgraph().getInputEdges().get(edge.getInputPortIndex()));
			}
		} else if (reader.isSubgraphOutputComponent() && reader.getParentMVGraph().getParent() != null) {
			affectedEdges.add(reader.getParentMVGraph().getParent().getOutputEdges().get(edge.getInputPortIndex()));
		} else {
			affectedEdges.addAll(reader.getOutputEdges().values());
			affectedEdges.addAll(reader.getInputEdges().values());
		}

		//find affected edges for writer component
		MVComponent writer = edge.getWriter();
		if (writer.isSubgraphComponent()) {
			if (writer.getSubgraph() != null) { //can be null if the subgraph reference is not valid
				affectedEdges.add(writer.getSubgraph().getOutputEdges().get(edge.getOutputPortIndex()));
			}
		} else if (writer.isSubgraphInputComponent() && writer.getParentMVGraph().getParent() != null) {
			affectedEdges.add(writer.getParentMVGraph().getParent().getInputEdges().get(edge.getOutputPortIndex()));
		} else {
			affectedEdges.addAll(writer.getOutputEdges().values());
			affectedEdges.addAll(writer.getInputEdges().values());
		}
		
		//find all edges which refers to the edge
		affectedEdges.addAll(edge.getMetadataRefInverted());
	}

	/**
	 * For all edges is also calculated which metadata would be propagated
	 * to this edge from neighbours, if the edge does not have any metadata explicitly assigned.
	 * It is useful only for designer purpose. Designer shows to user, which metadata
	 * would be on the edge, for "no metadata" option on the edge. 
	 */
	private void findAllNoMetadata() {
		//go through all edges with explicit metadata
		for (MVEdge edge : mvGraph.getMVEdges().values()) {
			if (edge.hasExplicitMetadata()) {
				//set virtual no metadata on the edge
				edge.setImplicitMetadata(null);
				//find the "no metadata"
				propagateMetadata();
				//remember the result
				edge.setNoMetadata(edge.getMetadata());
				//reset the resolver for next iteration
				reset();
			}
		}
	}
	
	private void reset() {
		mvGraph.reset();
	}
	
	/**
	 * Can be invoked from {@link MetadataProvider}s.
	 * @return suggested metadata for the given edge
	 */
	public MVMetadata findMetadata(Edge edge) {
		MVEdge mvEdge = mvGraph.getMVEdgeRecursive(edge);
		return (mvEdge != null) ? mvEdge.getMetadata() : null;
	}

	private MVMetadata findMetadata(MVEdge edge) {
		MVMetadata result = null;
		
		MVEdge referencedEdge = edge.getMetadataRef();
		if (referencedEdge != null && ReferenceState.isValidState(edge.getModel().getMetadataReferenceState())) {
			//metadata are dedicated by an edge reference
			result = referencedEdge.getMetadata();
			if (result != null) {
				result.setPriority(MVMetadata.HIGH_PRIORITY);
			} else if (isSelfReferenced(edge)) {
				//this is used for cyclic edge references, for example edge A refers to edge B and edge B refers back to edge A
				//in this case if we do not find metadata in the referenced edge, we try neighbour components
				//it is necessary to allow to make two way references, for example CustomJavaComponent which propagates metadata like SimpleCopy
				result = findMetadataFromNeighbours(edge);
			}
		} else if (!ReferenceState.isInvalidState(edge.getModel().getMetadataReferenceState())) {
			//otherwise try to ask your neighbours
			result = findMetadataFromNeighbours(edge);
		}
		
		return result;
	}
	
	private MVMetadata findMetadataFromNeighbours(MVEdge edge) {
		MVMetadata result = null;
		MVComponent originComponent = null;

		//check writer
		MVComponent writer = edge.getWriter();
		if (writer != null) {
			if (writer.isPassThrough()) {
				for (MVEdge inputEdge : writer.getInputEdges().values()) {
					result = combineMetadata(result, inputEdge.getMetadata());
					if (hasMaxPriority(result)) {
						break;
					}
				}
			} else {
				result = combineMetadata(result, writer.getDefaultOutputMetadata(edge.getOutputPortIndex(), this));
			}
			if (result != null) {
				originComponent = writer;
			}
		}

		//check reader
		if (!hasMaxPriority(result)) {
			MVComponent reader = edge.getReader();
			if (reader != null) {
				MVMetadata metadataFromWriter = result;
				if (reader.isPassThrough()) {
					for (MVEdge outputEdge : reader.getOutputEdges().values()) {
						result = combineMetadata(result, outputEdge.getMetadata());
						if (hasMaxPriority(result)) {
							break;
						}
					}
				} else {
					result = combineMetadata(result, reader.getDefaultInputMetadata(edge.getInputPortIndex(), this));
				}
				if (result != metadataFromWriter) {
					originComponent = reader;
				}
			}
		}
		
		if (result != null) {
			if (originComponent != null) {
				//construct metadata origin
				result.addToOriginPath(originComponent);
			} else {
				throw new IllegalStateException();
			}
		}
		
		return result;
	}

	/**
	 * @return metadata with higher priority; currentMetadata are preferred 
	 */
	private static MVMetadata combineMetadata(MVMetadata currentMetadata, MVMetadata newMetadata) {
		if (currentMetadata == null) {
			return newMetadata;
		} else if (newMetadata == null) {
			return currentMetadata;
		} else {
			return currentMetadata.getPriority() < newMetadata.getPriority() ? newMetadata : currentMetadata;
		}
	}

	/**
	 * @return true if the given metadata is not null and has maximal priority
	 */
	private static boolean hasMaxPriority(MVMetadata metadata) {
		return metadata != null && metadata.hasMaxPriority();
	}

	/**
	 * Creates MV model for given metadata.
	 * @param metadata wrapped metadata instance
	 * @param relatedGraphElement graph element which request this operation (it is necessary to detect logical parent graph of the metadata)
	 * @param identification string identifier of the metadata - should be unique in the scope of the relatedGraphElement
	 * @return MV model for given metadata
	 */
	public MVMetadata createMVMetadata(DataRecordMetadata metadata, IGraphElement relatedGraphElement, String identification) {
		return createMVMetadata(metadata, relatedGraphElement, identification, MVMetadata.DEFAULT_PRIORITY);
	}

	/**
	 * Creates MV model for given metadata.
	 * @param metadata metadata to be wrapped to MVMetadata (the instance shouldn't be shared, ID is changed)
	 * @param relatedGraphElement graph element which request this operation (it is necessary to detect logical parent graph of the metadata)
	 * @param identification string identifier of the metadata - should be unique in the scope of the relatedGraphElement
	 * @param priority priority of the metadata
	 * @return MV model for given metadata
	 */
	public MVMetadata createMVMetadata(DataRecordMetadata metadata, IGraphElement relatedGraphElement, String identification, int priority) {
		TransformationGraph parentEngineGraph = metadata.getGraph();
		if (parentEngineGraph == null) { //dynamic metadata
			if (relatedGraphElement != null) {
				parentEngineGraph = relatedGraphElement.getGraph();
				metadata.setId("__dynamic_metadata_" + relatedGraphElement.getId() + "_" + (identification != null ? identification : metadata.getName()));
			} else {
				metadata.setId("__dynamic_metadata_" + (identification != null ? identification : metadata.getName()));
			}
		}
		MVGraph parentMVGraph = mvGraph.getMVGraphRecursive(parentEngineGraph);
		return new MVEngineMetadata(metadata, parentMVGraph, priority);
	}

	/**
	 * @param edge
	 * @return MV representation of the given edge
	 */
	public MVEdge getMVEdge(Edge edge) {
		if (edge != null) {
			return mvGraph.getMVEdgeRecursive(edge);
		} else {
			return null;
		}
	}
	
	/**
	 * @return root graph for this metadata propagation
	 */
	public MVGraph getRootMVGraph() {
		return mvGraph;
	}

	/**
	 * @return true if the given edge refers an other edge which recursively refers back to the given edge, false otherwise
	 */
	private boolean isSelfReferenced(MVEdge edge) {
		MVEdge startEdge = edge;
		Set<MVEdge> visitedEdges = new HashSet<>();
		while (!visitedEdges.contains(edge)
				&& edge.getMetadataRef() != null
				&& ReferenceState.isValidState(edge.getModel().getMetadataReferenceState())) {
			visitedEdges.add(edge);
			edge = edge.getMetadataRef();
			if (edge == startEdge) {
				return true;
			}
		}
		return false;
	}

}
