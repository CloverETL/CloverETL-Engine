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
public class MVEngineEdge extends MVEngineGraphElement implements MVEdge {

	private static final long serialVersionUID = -7961493466822405222L;

	private MVMetadata implicitMetadata;
	
	private MVMetadata noMetadata;
	
	private boolean hasImplicitMetadata = false;

	MVEngineEdge(Edge engineEdge, MVGraph parentMVGraph) {
		super(engineEdge, parentMVGraph);
	}
	
	@Override
	public Edge getModel() {
		return (Edge) super.getModel();
	}
	
	@Override
	public void reset() {
		implicitMetadata = null;
		hasImplicitMetadata = false;
	}
	
	@Override
	public MVComponent getReader() {
		Node reader = getModel().getReader();
		return (reader != null) ? getParentMVGraph().getMVComponent(reader.getId()) : null;
	}

	@Override
	public MVComponent getWriter() {
		Node writer = getModel().getWriter();
		return (writer != null) ? getParentMVGraph().getMVComponent(writer.getId()) : null;
	}

	@Override
	public boolean hasMetadata() {
		return hasImplicitMetadata || getModel().getMetadata() != null;
	}

	@Override
	public boolean hasExplicitMetadata() {
		return getModel().getMetadata() != null;
	}

	@Override
	public boolean hasImplicitMetadata() {
		return hasImplicitMetadata;
	}

	@Override
	public MVMetadata getMetadata() {
		if (hasMetadata()) {
			if (hasImplicitMetadata) {
				if (implicitMetadata != null) {
					//duplicate is returned since propagated metadata can be changed
					//by propagation process - for example Reformat propagates metadata
					//from input to output ports, but priority of this metadata is decreased to ZERO level
					return implicitMetadata.duplicate();
				} else {
					return null;
				}
			} else {
				MVMetadata metadata = getParentMVGraph().createMVMetadata(getModel().getMetadata(), MVMetadata.HIGH_PRIORITY);
				metadata.addToOriginPath(this);
				return metadata;
			}
		} else {
			return null;
		}
	}

	@Override
	public void setImplicitMetadata(MVMetadata implicitMetadata) {
		hasImplicitMetadata = true;
		this.implicitMetadata = implicitMetadata;
	}
	
	@Override
	public MVMetadata getImplicitMetadata() {
		return implicitMetadata;
	}
	
	@Override
	public void unsetImplicitMetadata() {
		hasImplicitMetadata = false;
		this.implicitMetadata = null;
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
		return getModel().getOutputPortNumber();
	}

	@Override
	public int getInputPortIndex() {
		return getModel().getInputPortNumber();
	}

	@Override
	public MVGraph getParent() {
		return (MVGraph) super.getParent();
	}
	
	@Override
	public MVEdge getMetadataRef() {
		String metadataRef = getModel().getMetadataRef();
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
	
}
