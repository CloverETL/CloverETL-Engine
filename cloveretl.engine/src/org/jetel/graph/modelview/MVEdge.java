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

import org.jetel.graph.Edge;
import org.jetel.graph.MetadataPropagationResolver;

/**
 * This is general view to a edge. Two implementations are expected
 * - wrapper for engine and gui edge.
 * 
 * This model view is used by {@link MetadataPropagationResolver} and allows 
 * unified implementation for both engine and gui model.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 8. 2013
 */
public interface MVEdge {

	/**
	 * @return engine edge
	 */
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
	 * @return specific metadat assigned to this edge
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
	
}
