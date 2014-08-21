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
import org.jetel.graph.modelview.MVGraph;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.util.ReferenceUtils;
import org.jetel.util.string.StringUtils;

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

	private MVGraph parentMVGraph;
	
	MVEngineEdge(Edge engineEdge, MVGraph parentMVGraph) {
		if (engineEdge == null || parentMVGraph == null) {
			throw new IllegalArgumentException("MVEngineEdge init failed");
		}
		this.engineEdge = engineEdge;
		this.parentMVGraph = parentMVGraph;
	}
	
	@Override
	public Edge getModel() {
		return engineEdge;
	}
	
	@Override
	public String getId() {
		return engineEdge.getId();
	}
	
	@Override
	public void reset() {
		propagatedMetadata = null;
		hasPropagatedMetadata = false;
	}
	
	@Override
	public MVComponent getReader() {
		Node reader = engineEdge.getReader();
		return (reader != null) ? parentMVGraph.getMVComponent(reader.getId()) : null;
	}

	@Override
	public MVComponent getWriter() {
		Node writer = engineEdge.getWriter();
		return (writer != null) ? parentMVGraph.getMVComponent(writer.getId()) : null;
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
				if (propagatedMetadata != null) {
					//duplicate is returned since propagated metadata can be changed
					//by propagation process - for example Reformat propagates metadata
					//from input to output ports, but priority of this metadata is decreased to ZERO level
					return propagatedMetadata.duplicate();
				} else {
					return null;
				}
			} else {
				MVMetadata metadata = parentMVGraph.createMVMetadata(engineEdge.getMetadata(), MVMetadata.HIGH_PRIORITY);
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
	public MVGraph getParentMVGraph() {
		return parentMVGraph;
	}
	
	@Override
	public MVEdge getMetadataRef() {
		String metadataRef = engineEdge.getMetadataRef();
		if (!StringUtils.isEmpty(metadataRef)) {
			String edgeId = ReferenceUtils.getElementID(metadataRef);
			try {
				MVEdge edge = getParentMVGraph().getMVEdge(edgeId);
				return edge;
			} catch (Exception e) {
				//edge reference is somehow corrupted, let's ignore it
				return null;
			}
		} else {
			return null;
		}
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
