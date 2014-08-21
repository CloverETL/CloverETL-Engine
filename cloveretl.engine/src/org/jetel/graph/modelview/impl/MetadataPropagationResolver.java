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

import org.jetel.graph.Edge;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVGraph;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * General metadata propagation evaluator.
 * Metadata propagation algorithm is performed on model view (package org.jetel.graph.modelview).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 9. 2013
 */
public class MetadataPropagationResolver {
	
	/** Analysed graph */
	private MVGraph mvGraph;
	
	/**
	 * Creates resolver for automatic metadata propagation on the given graph.
	 */
	public MetadataPropagationResolver(MVGraph mvGraph) {
		this.mvGraph = mvGraph;
	}
	
	/**
	 * Main trigger for graph analysis.
	 */
	public void analyseGraph() {
		//find "no metadata" for all edges with direct metadata
		//"no metadata" is metadata, which would be used if the edge does not have the direct metadata associated
		findAllNoMetadata();

		//go through all edges and search metadata if necessary
		for (Edge edge : mvGraph.getModel().getEdges().values()) {
			MVEdge mvEdge = mvGraph.getMVEdge(edge.getId());
			MVMetadata mvMetadata = findMetadata(mvEdge);
			mvEdge.setPropagatedMetadata(mvMetadata);
		}
	}

	/**
	 * For all edges is also calculated which metadata would be propagated
	 * to this edge from neighbours, if the edge does not have any metadata directly assigned.
	 * It is useful only for designer purpose. Designer shows to user, which metadata
	 * would be on the edge, for "no metadata" option on the edge. 
	 */
	private void findAllNoMetadata() {
		//go through all edges without direct metadata
		for (Edge edge : mvGraph.getModel().getEdges().values()) {
			MVEdge mvEdge = mvGraph.getMVEdge(edge.getId());
			//set virtual no metadata on the edge
			mvEdge.setPropagatedMetadata(null);
			//find the "no metadata"
			MVMetadata noMetadata = findMetadataFromNeighbours(mvEdge);
			//remember the result
			mvEdge.setNoMetadata(noMetadata);
			//reset the resolver for next iteration
			reset();
		}
	}
	
	private void reset() {
		mvGraph.reset();
	}
	
	/**
	 * @return suggested metadata for given edge
	 */
	public MVMetadata findMetadata(Edge edge) {
		MVEdge mvEdge = mvGraph.getMVEdgeRecursive(edge);
		return (mvEdge != null) ? findMetadata(mvEdge) : null;
	}

	/**
	 * @return suggested metadata for given edge
	 */
	public MVMetadata findMetadata(MVEdge edge) {
		if (!edge.hasMetadata()) {
			edge.setPropagatedMetadata(null); //to avoid recursive search
			MVMetadata metadata = findMetadataInternal(edge);
			if (metadata != null) {
				//construct metadata origin path
				metadata.addToOriginPath(edge);
			}
			edge.unsetPropagatedMetadata();
			return metadata;
		} else {
			MVMetadata metadata = edge.getMetadata();
			return metadata != null ? metadata.duplicate() : null; //duplicate has to be returned due metadata origin path construction
		}
	}

	private static MVMetadata combineMetadata(MVMetadata currentMetadata, MVMetadata newMetadata) {
		if (currentMetadata == null) {
			return newMetadata;
		} else if (newMetadata == null) {
			return currentMetadata;
		} else {
			return currentMetadata.getPriority() < newMetadata.getPriority() ? newMetadata : currentMetadata;
		}
	}
	
	private MVMetadata findMetadataInternal(MVEdge edge) {
		MVMetadata result = null;
		
		MVEdge referencedEdge = edge.getMetadataRef();
		if (referencedEdge != null) {
			//metadata are dedicated by an edge reference
			result = findMetadata(referencedEdge);
			if (result != null) {
				result.addToOriginPath(referencedEdge);
			}
		} else {
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
					result = combineMetadata(result, findMetadata(inputEdge));
				}
			} else {
				result = combineMetadata(result, writer.getDefaultOutputMetadata(edge.getOutputPortIndex(), this));
			}
			if (result != null) {
				originComponent = writer;
			}
		}

		//check reader
		MVComponent reader = edge.getReader();
		if (reader != null) {
			MVMetadata metadataFromWriter = result;
			if (reader.isPassThrough()) {
				for (MVEdge outputEdge : reader.getOutputEdges().values()) {
					result = combineMetadata(result, findMetadata(outputEdge));
				}
			} else {
				result = combineMetadata(result, reader.getDefaultInputMetadata(edge.getInputPortIndex(), this));
			}
			if (result != metadataFromWriter) {
				originComponent = reader;
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
	 * @param metadata wrapped metadata instance
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
				//shouldn't be the metadata duplicated ???
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
	
}
