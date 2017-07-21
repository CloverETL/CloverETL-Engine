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
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;

/**
 * Utility class for subgraph related code.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.4.2013
 */
public class SubgraphUtils {

	/**
	 * License product id.
	 */
	public static final String PRODUCT_ID = "com.cloveretl.server";
	/**
	 * License product id for Runtime.
	 */
	public static final String RUNTIME_PRODUCT_ID = "com.cloveretl.runtime";
	
	/**
	 * License feature id for subgraphs.
	 */
	public static final String FEATURE_ID = "com.cloveretl.server.subgraph";

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
     * Name of single type of component property (and graph parameters) for fileURL.
     * Graph parameters with this type is handled in special way in Subgraph component.
     * Relative paths are converted to absolute, see Subgraph.applySubgraphParameter().
     */
    public static final String FILE_URL_SINGLE_TYPE = "file";
	/**
	 * Prefix of custom attributes of Subgraph component.
	 */
	public static final String CUSTOM_SUBGRAPH_ATTRIBUTE_PREFIX = "__";

	/**
	 * @return true iff given component is SubgraphInput or SubgraphOutput component
	 */
	public static boolean isSubgraphInputOutputComponent(String componentType){
		return SUBGRAPH_INPUT_TYPE.equals(componentType) || SUBGRAPH_OUTPUT_TYPE.equals(componentType);
	}
	
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
		return subgraphEdge.getGraph().getRuntimeJobType().isSubJob()
				&& subgraphEdge.getGraph().getRuntimeJobType().isGraph() //jobflows do not share edges to avoid distorted logging of token tracked 
				&& !subgraphEdge.isEdgeDebugging()
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
		return subgraphEdge.getGraph().getRuntimeJobType().isSubJob()
				&& subgraphEdge.getGraph().getRuntimeJobType().isGraph() //jobflows do not share edges to avoid distorted logging of token tracked 
				&& !parentGraphEdge.isEdgeDebugging()
				&& subgraphEdge.getEdgeType() != EdgeTypeEnum.PHASE_CONNECTION
				&& !SubgraphUtils.isSubJobInputComponent(subgraphEdge.getWriter().getType()); //edges directly interconnect SubgraphInput and SubgraphOutput cannot be share from both sides
	}
	
	/**
	 * Checks whether the given attribute name has "__foo" format,
	 * which is indication of custom subgraph attribute
	 * @return true if the given name of an attribute is custom subgraph attribute (prefixed with double underscores)  
	 */
	public static boolean isCustomSubgraphAttribute(String attributeName) {
		return attributeName.startsWith(CUSTOM_SUBGRAPH_ATTRIBUTE_PREFIX);
	}
	
	/**
	 * Converts the given name of custom subgraph attribute to name of respective
	 * public graph parameter.
	 */
	public static String getPublicGraphParameterName(String customSubgraphAttribute) {
		return customSubgraphAttribute.substring(CUSTOM_SUBGRAPH_ATTRIBUTE_PREFIX.length());
	}

	/**
	 * Converts the given name of public graph parameter to respective
	 * name of custom subgraph attribute.
	 */
	public static String getCustomSubgraphAttribute(String publicGraphParameterName) {
		return CUSTOM_SUBGRAPH_ATTRIBUTE_PREFIX + publicGraphParameterName;
	}
	
	/**
	 * Finds SubgraphInput component in the graph. Returns null instead of throwing exception if component is missing.
	 */
	public static Node getSubgraphInput(TransformationGraph graph) {
		try {
			return graph.getSubgraphInputComponent();
		} catch (JetelRuntimeException e) {
			return null;
		}
	}

	/**
	 * Finds SubgraphOutput component in the graph. Returns null instead of throwing exception if component is missing.
	 */
	public static Node getSubgraphOutput(TransformationGraph graph) {
		try {
			return graph.getSubgraphOutputComponent();
		} catch (JetelRuntimeException e) {
			return null;
		}
	}

}
