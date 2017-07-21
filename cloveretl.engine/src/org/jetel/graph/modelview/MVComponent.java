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

import java.util.Map;

import org.jetel.graph.Node;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;

/**
 * This is general model view to a graph component.
 * This model view is used by {@link MetadataPropagationResolver}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 8. 2013
 */
public interface MVComponent extends MVGraphElement {

	/**
	 * @return engine component
	 */
	@Override
	public Node getModel();
	
	/**
	 * @return input edges in map container, where key is port index
	 */
	public Map<Integer, MVEdge> getInputEdges();
	
	/**
	 * @return output edges in map container, where key is port index
	 */
	public Map<Integer, MVEdge> getOutputEdges();
	
	/**
	 * @return is the component passThrough - should be metadata automatically
	 * propagated from first input port to all output ports
	 */
	public boolean isPassThrough();
	
	/**
	 * Instance of propagation resolver is passed to be able to provide
	 * dynamic metadata based on other ports.
	 * @return metadata suggested by this component for requested output port index
	 */
	public MVMetadata getDefaultOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver);

	/**
	 * Instance of propagation resolver is passed to be able to provide
	 * dynamic metadata based on other ports.
	 * @return metadata suggested by this component for requested input port index
	 */
	public MVMetadata getDefaultInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver);
	
	/**
	 * @return parent graph of this component
	 */
	public MVGraph getParentMVGraph();

}
