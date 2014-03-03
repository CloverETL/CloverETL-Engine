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
package org.jetel.util;

import org.jetel.enums.EdgeTypeEnum;
import org.jetel.graph.Edge;

/**
 * Utility class for subgraph related code.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.4.2013
 */
public class SubgraphUtils {

	/** Type of SubgraphInput component. */
	public static final String SUBGRAPH_INPUT_TYPE = "SUBGRAPH_INPUT";
	/** Type of SubgraphOutput component. */
	public static final String SUBGRAPH_OUTPUT_TYPE = "SUBGRAPH_OUTPUT";

	/** Type of SubjobflowInput component. */
	public static final String SUBJOBFLOW_INPUT_TYPE = "SUBJOBFLOW_INPUT";
	/** Type of SubjobflowOutput component. */
	public static final String SUBJOBFLOW_OUTPUT_TYPE = "SUBJOBFLOW_OUTPUT";

	/** Type of Subgraph component. */
	public static final String SUBGRAPH_TYPE = "SUBGRAPH";

	/** Type of Subjobflow component. */
	public static final String SUBJOBFLOW_TYPE = "SUBJOBFLOW";

    /** the name of an XML attribute used to pass a URL specified the executed subgraph */
    public static final String XML_JOB_URL_ATTRIBUTE = "jobURL";

	/**
	 * @return true if and only if the given component type is SubgraphInput or SubjobflowInput component.
	 */
	public static boolean isSubJobInputComponent(String componentType) {
		return SUBGRAPH_INPUT_TYPE.equals(componentType) || SUBJOBFLOW_INPUT_TYPE.equals(componentType);
	}

	/**
	 * @return true if and only if the given component type is SubgraphOutput or SubjobflowOutput component.
	 */
	public static boolean isSubJobOutputComponent(String componentType) {
		return SUBGRAPH_OUTPUT_TYPE.equals(componentType) || SUBJOBFLOW_OUTPUT_TYPE.equals(componentType);
	}

	/**
	 * @return true if and only if the given component type is {@link #isSubJobInputComponent(String)} or {@link #isSubJobOutputComponent(String)}
	 */
	public static boolean isSubJobInputOutputComponent(String componentType) {
		return isSubJobInputComponent(componentType) || isSubJobOutputComponent(componentType);
	}
	
	/**
	 * @return true if and only if the given component type is Subgraph or Subjobflow component.
	 */
	public static boolean isSubJobComponent(String componentType) {
		return SUBGRAPH_TYPE.equals(componentType) || SUBJOBFLOW_TYPE.equals(componentType);
	}

	/**
	 * Checks whether output edge of SubgraphInput component can share EdgeBase
	 * with corresponding edge in parent graph. In regular cases, it is possible and
	 * highly recommended due performance gain. But in case edge debugging is turned on,
	 * sharing is not possible. Remote and phase edges cannot be shared as well.
	 * @param subgraphEdge an output edge from SubgraphInput component
	 * @param parentGraphEdge corresponding edge from parent graph
	 * @return true if and only if the edge base from parentEdge can be shared with localEdge
	 */
	public static boolean isSubgraphInputEdgeShared(Edge subgraphEdge, Edge parentGraphEdge) {
		return subgraphEdge.getGraph().getRuntimeContext().isSubJob()
				&& !subgraphEdge.isDebugMode()
				&& subgraphEdge.getEdgeType() != EdgeTypeEnum.L_REMOTE
				&& subgraphEdge.getEdgeType() != EdgeTypeEnum.PHASE_CONNECTION;
	}

	/**
	 * Checks whether input edge of SubgraphOutput component can share EdgeBase
	 * with corresponding edge in parent graph. In regular cases, it is possible and
	 * highly recommended due performance gain. But in case edge debugging is turned on,
	 * sharing is not possible. Phase edges cannot be shared as well.
	 * @param subgraphEdge an input edge from SubgraphOutput component
	 * @param parentEdge corresponding edge from parent graph
	 * @return true if and only if the edge base from parentEdge can be shared with localEdge
	 */
	public static boolean isSubgraphOutputEdgeShared(Edge subgraphEdge, Edge parentGraphEdge) {
		return subgraphEdge.getGraph().getRuntimeContext().isSubJob()
				&& !parentGraphEdge.isDebugMode()
				&& subgraphEdge.getEdgeType() != EdgeTypeEnum.PHASE_CONNECTION;
	}
	
}
