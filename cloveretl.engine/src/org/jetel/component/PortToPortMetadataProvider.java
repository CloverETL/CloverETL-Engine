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

import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;

/**
 * This metadata provider ensures that metadata from n-th input port
 * are delegated to n-th output port and vice versa.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4. 10. 2013
 */
public class PortToPortMetadataProvider implements ComponentMetadataProvider {
	
	protected Node component;
	
	@Override
	public MVMetadata getInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		OutputPort outputPort = component.getOutputPort(portIndex);
		if (outputPort != null) {
			return metadataPropagationResolver.findMetadata(outputPort.getEdge());
		} else {
			return null;
		}
	}

	@Override
	public MVMetadata getOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		InputPort inputPort = component.getInputPort(portIndex);
		if (inputPort != null) {
			return metadataPropagationResolver.findMetadata(inputPort.getEdge());
		} else {
			return null;
		}
	}

	@Override
	public void setComponent(Node component) {
		this.component = component;
	}
	
}
