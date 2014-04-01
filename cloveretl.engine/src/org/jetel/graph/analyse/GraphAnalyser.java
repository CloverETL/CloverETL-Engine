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
package org.jetel.graph.analyse;

import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.TransformationGraph;

/**
 * Utility class which is used for graph analysis. The only method
 * {@link #analyseGraph(TransformationGraph)} detects type of all edges.
 * Necessary for detection of PHASE and BUFFERED edges.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.12.2012
 */
public class GraphAnalyser {

	public static void analyseGraph(TransformationGraph graph) {
		//detect empty graphs
		if (graph.getNodes().isEmpty()) {
			throw new JetelRuntimeException("Job without components cannot be executed.");
		}

		//first of all find the phase edges
		analysePhaseEdges(graph);

		//let's find cycles of relationships in the graph and interrupted them by buffered edges to avoid deadlocks
		GraphCycleInspector graphCycleInspector = new GraphCycleInspector(new SingleGraphProvider(graph));
		graphCycleInspector.inspectGraph();
	}

	private static void analysePhaseEdges(TransformationGraph graph) {
		Phase readerPhase;
		Phase writerPhase;

		// analyse edges (whether they need to be buffered and put them into proper phases
		// edges connecting nodes from two different phases has to be put into both phases
		for (Edge edge : graph.getEdges().values()) {
			Node reader = edge.getReader(); //can be null for remote edges
			Node writer = edge.getWriter(); //can be null for remote edges
			readerPhase = reader != null ? reader.getPhase() : null;
			writerPhase = writer != null ? writer.getPhase() : null;
			if (readerPhase.getPhaseNum() > writerPhase.getPhaseNum()) {
				// edge connecting two nodes belonging to different phases
				// has to be buffered
				edge.setEdgeType(EdgeTypeEnum.PHASE_CONNECTION);
			}
		}
	}

}
