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
package org.jetel.graph.modelview;

import java.util.List;

import org.jetel.graph.Edge;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;

/**
 * This is general model view to an edge.
 * This model view is used by {@link MetadataPropagationResolver}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 8. 2013
 */
public interface MVEdge extends MVGraphElement {

	/**
	 * @return engine edge
	 */
	@Override
	public Edge getModel();
	
	/**
	 * @return reader (data producer) of this edge
	 */
	public MVComponent getReader();
	
	/**
	 * @return writer (data consumer) of this edge
	 */
	public MVComponent getWriter();
	
	/**
	 * @return true if a specific metadata is assigned to this edge
	 */
	public boolean hasMetadata();
	
	/**
	 * @return true if the edge has metadata directly assigned by user
	 */
	public boolean hasExplicitMetadata();

	/**
	 * @return true if the edge is going to use some implicit metadata (propagated metadata) 
	 */
	public boolean hasImplicitMetadata();

	/**
	 * @return specific metadata assigned to this edge
	 */
	public MVMetadata getMetadata();
	
	/**
	 * @return port index of this edge, where this edge is attached to its writer component
	 */
	public int getOutputPortIndex();

	/**
	 * @return port index of this edge, where this edge is attached to its reader component
	 */
	public int getInputPortIndex();

	/**
	 * Sets metadata to this edge which has been automatically propagated from neighbours.
	 * @param implicitMetadata propagated metadata
	 */
	public void setImplicitMetadata(MVMetadata implicitMetadata);

	/**
	 * @return implicit metadata if any (propagated metadata)
	 */
	public MVMetadata getImplicitMetadata();
	
	/**
	 * Sets 'no metadata' for this edge. The 'no metadata' are metadata which
	 * would be used for this edge if no direct metadata is set on this edge.
	 * @param noMetadata
	 */
	public void setNoMetadata(MVMetadata noMetadata);
	
	/**
	 * @return 'no metadata' for this edge - metadata which would be used if no direct metadata is set on this edge
	 */
	public MVMetadata getNoMetadata();

	/**
	 * @return referenced edge, from where metadata should be derived
	 */
	public MVEdge getMetadataRef();

	/**
	 * @return list of edges which refers to this edge; all the edges derive metadata from this edge
	 */
	public List<MVEdge> getMetadataRefInverted();

	/**
	 * Returns parent graph.
	 */
	@Override
	public MVGraph getParent();
	
}
