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

import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVMetadata;

/**
 * General model wrapper for engine edge ({@link Edge}).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19. 9. 2013
 */
public class MVEngineEdge implements MVEdge {

	private Edge engineEdge;
	
	private MVMetadata propagatedMetadata;
	
	private MVMetadata noMetadata;
	
	private boolean hasPropagatedMetadata = false;

	private MetadataPropagationResolver metadataPropagationResolver;
	
	MVEngineEdge(Edge engineEdge, MetadataPropagationResolver metadataPropagationResolver) {
		this.engineEdge = engineEdge;
		this.metadataPropagationResolver = metadataPropagationResolver;
	}
	
	@Override
	public Edge getModel() {
		return engineEdge;
	}
	
	@Override
	public MVComponent getReader() {
		Node reader = engineEdge.getReader();
		return (reader != null) ? metadataPropagationResolver.getOrCreateMVComponent(reader) : null;
	}

	@Override
	public MVComponent getWriter() {
		Node writer = engineEdge.getWriter();
		return (writer != null) ? metadataPropagationResolver.getOrCreateMVComponent(writer) : null;
	}

	@Override
	public boolean hasMetadata() {
		return hasPropagatedMetadata || engineEdge.getMetadata() != null;
	}

	@Override
	public boolean hasMetadataDirect() {
		return engineEdge.getMetadata() != null;
	}
	
	@Override
	public MVMetadata getMetadata() {
		if (hasMetadata()) {
			if (hasPropagatedMetadata) {
				return propagatedMetadata;
			} else {
				MVMetadata metadata = metadataPropagationResolver.getOrCreateMVMetadata(engineEdge.getMetadata(), MVMetadata.HIGH_PRIORITY);
				metadata.addToOriginPath(this);
				return metadata;
			}
		} else {
			return null;
		}
	}

	@Override
	public void setPropagatedMetadata(MVMetadata propagatedMetadata) {
		hasPropagatedMetadata = true;
		this.propagatedMetadata = propagatedMetadata;
	}
	
	@Override
	public void unsetPropagatedMetadata() {
		hasPropagatedMetadata = false;
		this.propagatedMetadata = null;
	}
	
	@Override
	public void setNoMetadata(MVMetadata noMetadata) {
		this.noMetadata = noMetadata;
	}
	
	@Override
	public MVMetadata getNoMetadata() {
		return noMetadata;
	}
	
	@Override
	public int getOutputPortIndex() {
		return engineEdge.getOutputPortNumber();
	}

	@Override
	public int getInputPortIndex() {
		return engineEdge.getInputPortNumber();
	}

	@Override
	public int hashCode() {
		return engineEdge.hashCodeIdentity();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof MVEngineEdge)) { 
			return false;
		}
		return engineEdge == ((MVEngineEdge) obj).engineEdge;
	}

	@Override
	public String toString() {
		return engineEdge.toString();
	}
	
}
