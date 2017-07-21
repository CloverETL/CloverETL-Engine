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

import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;

/**
 * This is basic implementation of MetadataProvider which has
 * static set of provided metadata.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25. 2. 2014
 */
public class StaticMetadataProvider implements SerializableMetadataProvider {
	
	private static final long serialVersionUID = -7111240454722386558L;
	
	private MVMetadata[] inputMetadata;
	private MVMetadata[] outputMetadata;

	public StaticMetadataProvider(MVMetadata[] inputMetadata, MVMetadata[] outputMetadata) {
		this.inputMetadata = inputMetadata;
		this.outputMetadata = outputMetadata;
	}
	
	@Override
	public MVMetadata getOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		if (portIndex < outputMetadata.length && outputMetadata[portIndex] != null) {
			return metadataPropagationResolver.createMVMetadata(outputMetadata[portIndex].getModel(), null, "output_" + portIndex, outputMetadata[portIndex].getPriority());
		} else {
			return null;
		}
	}
	
	@Override
	public MVMetadata getInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		if (portIndex < inputMetadata.length && inputMetadata[portIndex] != null) {
			return metadataPropagationResolver.createMVMetadata(inputMetadata[portIndex].getModel(), null, "input_" + portIndex, inputMetadata[portIndex].getPriority());
		} else {
			return null;
		}
	}
	
}
