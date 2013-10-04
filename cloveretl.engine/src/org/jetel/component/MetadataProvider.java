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
package org.jetel.component;

import org.jetel.graph.MetadataPropagationResolver;
import org.jetel.graph.Node;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Components can provide default metadata for theirs input and output ports.
 * These metadata are automatically propagated to connected edges if the edge does not have
 * defined metadata. Propagation resolver is available to recursive
 * metadata searching. For example if you want to say, that metadata 
 * on requested port are identical with the metadata on an other port -
 * use {@link MetadataPropagationResolver} to find the metadata.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19. 9. 2013
 */
public interface MetadataProvider {
	
	/**
	 * @return default metadata for input port with given index
	 */
	public DataRecordMetadata getInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver);
	
	/**
	 * @return default metadata for output port with given index
	 */
	public DataRecordMetadata getOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver);

}
