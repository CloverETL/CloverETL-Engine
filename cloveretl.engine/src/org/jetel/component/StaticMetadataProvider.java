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

import java.io.Serializable;

import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This is basic implementation of MetadataProvider which has
 * static set of provided metadata. This class is serializable.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25. 2. 2014
 */
public class StaticMetadataProvider implements MetadataProvider, Serializable {
	
	private static final long serialVersionUID = -8436473524226111583L;
	
	private DataRecordMetadata[] inputMetadata;
	private DataRecordMetadata[] outputMetadata;

	public StaticMetadataProvider(DataRecordMetadata[] inputMetadata, DataRecordMetadata[] outputMetadata) {
		this.inputMetadata = inputMetadata;
		this.outputMetadata = outputMetadata;
	}
	
	@Override
	public MVMetadata getOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		if (portIndex < outputMetadata.length && outputMetadata[portIndex] != null) {
			return metadataPropagationResolver.createMVMetadata(outputMetadata[portIndex], null, "output_" + portIndex, MVMetadata.TOP_PRIORITY);
		} else {
			return null;
		}
	}
	
	@Override
	public MVMetadata getInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		if (portIndex < inputMetadata.length && inputMetadata[portIndex] != null) {
			return metadataPropagationResolver.createMVMetadata(inputMetadata[portIndex], null, "input_" + portIndex, MVMetadata.TOP_PRIORITY);
		} else {
			return null;
		}
	}
	
}
