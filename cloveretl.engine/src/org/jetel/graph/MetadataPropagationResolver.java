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

import java.util.HashMap;
import java.util.Map;

import org.jetel.component.MetadataProvider;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MVEngineEdge;

/**
 * General metadata propagation evaluator.
 * Metadata propagation algorithm is performed on model view (package org.jetel.graph.modelview),
 * which allows to run metadata propagation on engine and gui model using same implementation.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 9. 2013
 */
public class MetadataPropagationResolver {
	
	/** Set of already visited edges with associated metadata. */
	private Map<MVEdge, MVMetadata> visitedEdges = new HashMap<MVEdge, MVMetadata>(); 

	/**
	 * This view edge factory is used by {@link #findMetadata(Edge)}.
	 * This is necessary for {@link MetadataPropagationResolver}
	 * used from {@link MetadataProvider}.
	 */
	private MVEdgeFactory edgeFactory;
	
	/**
	 * @param edgeFactory factory which converts regular engine {@link Edge} to correct {@link MVEdge} implementation.
	 */
	public MetadataPropagationResolver(MVEdgeFactory edgeFactory) {
		this.edgeFactory = edgeFactory;
	}
	
	/**
	 * @return suggested metadata for given edge
	 */
	public MVMetadata findMetadata(Edge edge) {
		return findMetadata(createEdge(edge));
	}

	private MVEdge createEdge(Edge edge) {
		return edgeFactory.create(edge);
	}

	/**
	 * @return suggested metadata for given edge
	 */
	public MVMetadata findMetadata(MVEdge edge) {
		if (!visitedEdges.containsKey(edge)) {
			visitedEdges.put(edge, null);
			MVMetadata metadata;
			metadata = findMetadataInternal(edge);
			visitedEdges.put(edge, metadata);
			return metadata;
		} else {
			return visitedEdges.get(edge);
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
		if (!edge.hasMetadata()) {
			MVMetadata result = null;
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
			}

			//check reader
			MVComponent reader = edge.getReader();
			if (reader != null) {
				if (reader.isPassThrough()) {
					for (MVEdge outputEdge : reader.getOutputEdges().values()) {
						result = combineMetadata(result, findMetadata(outputEdge));
					}
				} else {
					result = combineMetadata(result, reader.getDefaultInputMetadata(edge.getInputPortIndex(), this));
				}
			}
			
			return result;
		} else {
			return edge.getMetadata();
		}
	}
	
	/**
	 * This factory allows to convert engine {@link Edge} to correct {@link MVEdge} implementation.
	 */
	public static interface MVEdgeFactory {
		public MVEdge create(Edge edge);
	}
	
	public static class EngineMVEdgeFactory implements MVEdgeFactory {
		@Override
		public MVEdge create(Edge edge) {
			return new MVEngineEdge((Edge) edge);
		}
	}

	/**
	 * Resets this resolver for next usage.
	 */
	public void reset() {
		visitedEdges.clear();
	}
	
}
