/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.graph.analyse;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.jetel.enums.EnabledEnum;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphAnalyzer;
import org.jetel.util.SubGraphUtils;

/**
 * This is preliminary implementation of sub-graph analysis. Can be changed in the future.
 * All components before SubGraphInput and after SubGraphOutput are removed.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.4.2013
 */
public class SubGraphAnalyser {

	public static void adjustGraph(TransformationGraph graph) {
		for (Node component : graph.getNodes().values()) {
			if (SubGraphUtils.isSubGraphInput(component.getType())) {
				List<Node> precedentNodes = TransformationGraphAnalyzer.findPrecedentNodesRecursive(component, null);
				List<Node> followingNodes = TransformationGraphAnalyzer.findFollowingNodesRecursive(component, null);
				if (!CollectionUtils.intersection(precedentNodes, followingNodes).isEmpty()) {
					throw new JetelRuntimeException("Invalid subgraph layout. A component preceding the SubGraphInput component is probably connected with a component following SubGraphInput.");
				}
				for (Node precedentNode : precedentNodes) {
					precedentNode.setEnabled(EnabledEnum.DISABLED);
				}
			}
			if (SubGraphUtils.isSubGraphOutput(component.getType())) {
				List<Node> followingNodes = TransformationGraphAnalyzer.findFollowingNodesRecursive(component, null);
				List<Node> precedentNodes = TransformationGraphAnalyzer.findPrecedentNodesRecursive(component, null);
				if (!CollectionUtils.intersection(precedentNodes, followingNodes).isEmpty()) {
					throw new JetelRuntimeException("Invalid subgraph layout. A component following the SubGraphOutput component is probably connected with a component preceding SubGraphOutput.");
				}
				for (Node followingNode : followingNodes) {
					followingNode.setEnabled(EnabledEnum.DISABLED);
				}
			}
		}
		
        //remove disabled components and their edges
        try {
			TransformationGraphAnalyzer.disableNodesInPhases(graph);
		} catch (GraphConfigurationException e) {
			throw new JetelRuntimeException("Failed to remove disabled/pass-through nodes from sub-graph.", e);
		}
	}
	
}
