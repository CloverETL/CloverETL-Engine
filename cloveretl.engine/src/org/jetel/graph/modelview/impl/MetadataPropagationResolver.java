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
import java.util.Map.Entry;

import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
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
	private TransformationGraph graph;
	
	/** Cache for edge model view (MV) entities. */
	private Map<IdentityHashKey<Edge>, MVEdge> mvEdges = new HashMap<>();

	/** Cache for component model view (MV) entities. */
	private Map<IdentityHashKey<Node>, MVComponent> mvComponents = new HashMap<>();
	
	/**
	 * Creates resolver for automatic metadata propagation on the given graph.
	 */
	public MetadataPropagationResolver(TransformationGraph graph) {
		this.graph = graph;
	}
	
	/**
	 * Main trigger for graph analysis.
	 */
	public void analyseGraph() {
		//find "no metadata" for all edges with direct metadata
		//"no metadata" is metadata, which would be used if the edge does not have the direct metadata associated
		findAllNoMetadata();

		//go through all edges and search metadata if necessary
		for (Edge edge : graph.getEdges().values()) {
			MVEdge mvEdge = getOrCreateMVEdge(edge);
			MVMetadata mvMetadata = findMetadata(edge);
			mvEdge.setPropagatedMetadata(mvMetadata);
		}
	}

	/**
	 * For all edges with direct metadata is also calculated which metadata would be propagated
	 * to this edge, if the edge does not have metadata directly assigned.
	 * It is useful only for designer purpose. Designer shows to user, which metadata
	 * will be on the edge, for "no metadata" option on the edge. 
	 */
	private void findAllNoMetadata() {
		//go through all edges without direct metadata
		Map<Edge, MVMetadata> noMetadatas = new HashMap<>();
		for (Edge edge : graph.getEdges().values()) {
			MVEdge mvEdge = getOrCreateMVEdge(edge);
			if (mvEdge.hasMetadataDirect()) {
				//set virtual no metadata on the edge
				mvEdge.setPropagatedMetadata(null);
				//find the "no metadata"
				MVMetadata noMetadata = findMetadataInternal(mvEdge);
				//remember the result
				noMetadatas.put(edge, noMetadata);
				//reset the resolver for next iteration
				reset();
			}
		}
		
		//apply the results
		for (Entry<Edge, MVMetadata> entry : noMetadatas.entrySet()) {
			getOrCreateMVEdge(entry.getKey()).setNoMetadata(entry.getValue());
		}
	}
	
	private void reset() {
		mvEdges.clear();
		mvComponents.clear();
	}
	
	/**
	 * @return suggested metadata for given edge
	 */
	public MVMetadata findMetadata(Edge edge) {
		return findMetadata(getOrCreateMVEdge(edge));
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
	 * Creates new model view instance for given edge or return 
	 * previously created instance from a cache.
	 * @param edge wrapped edge
	 * @return model view wrapper for the given edge
	 */
	public MVEdge getOrCreateMVEdge(Edge edge) {
		IdentityHashKey<Edge> key = IdentityHashKey.create(edge);
		if (!mvEdges.containsKey(key)) {
			MVEdge mvEdge = new MVEngineEdge(edge, this);
			mvEdges.put(key, mvEdge);
			return mvEdge;
		} else {
			return mvEdges.get(key);
		}
	}

	/**
	 * Creates new model view instance for given component or return 
	 * previously created instance from a cache.
	 * @param component wrapped component
	 * @return model view wrapper for the given component
	 */
	public MVComponent getOrCreateMVComponent(Node component) {
		IdentityHashKey<Node> key = IdentityHashKey.create(component);
		if (!mvComponents.containsKey(key)) {
			MVComponent mvComponent = new MVEngineComponent(component, this);
			mvComponents.put(key, mvComponent);
			return mvComponent;
		} else {
			return mvComponents.get(key);
		}
	}

	/**
	 * Creates new model view instance for given metadata.
	 * @param metadata wrapped metadata
	 * @return model view wrapper for the given metadata
	 */
	public MVMetadata getOrCreateMVMetadata(DataRecordMetadata metadata) {
		return new MVEngineMetadata(metadata);
	}

	/**
	 * Creates new model view instance for given metadata.
	 * @param metadata wrapped metadata
	 * @param priority metadata priority
	 * @return model view wrapper for the given metadata
	 */
	public MVMetadata getOrCreateMVMetadata(DataRecordMetadata metadata, int priority) {
		return new MVEngineMetadata(metadata, priority);
	}
	
	/**
	 * This is simple class which wraps an object and the wrapper can
	 * be used in collections to change the object hashCode() and equals()
	 * methods to 'identity' manner.
	 */
	private static class IdentityHashKey<T> {
		private T key;
		
		public static <T> IdentityHashKey<T> create(T key) {
			return new IdentityHashKey<>(key);
		}
		
		public IdentityHashKey(T key) {
			this.key = key;
		}
		
		@Override
		public int hashCode() {
			return System.identityHashCode(key);
		}
		
		@Override
		public boolean equals(Object obj) {
			return obj instanceof IdentityHashKey && ((IdentityHashKey<?>) obj).key == key;
		}
	}
	
}
