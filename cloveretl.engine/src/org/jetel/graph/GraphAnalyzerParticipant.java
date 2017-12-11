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
package org.jetel.graph;

import org.jetel.exception.GraphConfigurationException;
import org.jetel.graph.runtime.GraphRuntimeContext;

/**
 * Extension point for analyse graph phase
 * 
 * @author adamekl (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20. 11. 2017
 */
public interface GraphAnalyzerParticipant {
	
	/**
	 * Method realize custom analyze of graph. Method is called from GraphAnalyzer after metadata propagate
	 *
	 * @param graph the graph
	 * @param runtimeContext the runtime context
	 * @param propagateMetadata the propagate metadata
	 * @throws GraphConfigurationException the graph configuration exception
	 */
	void afterPropagate(TransformationGraph graph, GraphRuntimeContext runtimeContext, boolean propagateMetadata)
			throws GraphConfigurationException;

}
