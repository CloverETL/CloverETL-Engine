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

import java.util.HashSet;
import java.util.Set;

import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVMetadata;

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
public class MetadataPropagationResolver<T> {
	
	/**
	 * @return metadata for given edge
	 */
	public MVMetadata<T> findMetadata(MVEdge<T> edge) {
		return findMetadata(edge, new HashSet<MVComponent<T>>());
	}

	private MVMetadata<T> findMetadata(MVEdge<T> edge, Set<MVComponent<T>> visitedComponents) {
		if (!edge.hasMetadata()) {
			//check writer
			MVComponent<T> writer = edge.getWriter();
			if (writer != null && !visitedComponents.contains(writer)) {
				visitedComponents.add(writer);
				if (writer.isPassThrough()) {
					if (writer.getInputEdges().size() > 0) {
						MVMetadata<T> result = findMetadata(writer.getInputEdges().get(0), visitedComponents);
						if (result != null) {
							return result;
						}
					}
				} else {
					MVMetadata<T> result = writer.getDefaultOutputMetadata(edge.getOutputPortIndex());
					if (result != null) {
						return result;
					}
				}
			}

			//check reader
			MVComponent<T> reader = edge.getReader();
			if (reader != null && !visitedComponents.contains(reader)) {
				visitedComponents.add(reader);
				if (reader.isPassThrough()) {
					if (reader.getOutputEdges().size() > 0) {
						MVMetadata<T> result = findMetadata(reader.getOutputEdges().get(0), visitedComponents);
						if (result != null) {
							return result;
						}
					}
				} else {
					MVMetadata<T> result = reader.getDefaultInputMetadata(edge.getInputPortIndex());
					if (result != null) {
						return result;
					}
				}
			}
			
			return null;
		} else {
			return edge.getMetadata();
		}
	}
	
}
