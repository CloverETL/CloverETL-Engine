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

import org.jetel.graph.MetadataPropagationResolver;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This is general view to a metadata. Two implementations are expected
 * - wrapper for engine and gui metadata.
 * 
 * This model view is used by {@link MetadataPropagationResolver} and allows 
 * unified implementation for both engine and gui model.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19. 9. 2013
 */
public interface MVMetadata {

	/**
	 * This high priority is used when metadata is defined directly on edge.
	 */
	public static final int HIGH_PRIORITY = 10;
	
	/**
	 * This is default priority.
	 */
	public static final int LOW_PRIORITY = 1;
	
	/**
	 * @return wrapped metadata, either DataRecordMetadata or GraphMetadata
	 */
	public DataRecordMetadata getMetadata();
	
	/**
	 * Priority of metadata is used to decide which metadata should be used.
	 * Metadata decision for an edge - look left, look right and take metadata
	 * with higher priority.
	 * @see MetadataPropagationResolver
	 * @return metadata priority
	 */
	public int getPriority();
	
}
