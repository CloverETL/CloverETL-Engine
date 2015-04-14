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

import java.util.ArrayList;
import java.util.List;

/**
 * This class should prepare all built-in graph parameters.
 * Regular graph parameters are defined by user in graph specification (xml), but
 * graph parameters provided by this class are presented in graph by default.
 * For now the class provide only one type of built-in graph parameters, which
 * specify whether a subgraph input/output port is connected. The graph parameters has
 * following pattern:
 * ENABLE_ON_INPUT_PORT_0_CONNECTED, DISABLE_ON_OUTPUT_PORT_1_CONNECTED, ...
 *   
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3. 3. 2015
 * @see com.cloveretl.gui.model.BuiltInGraphParameters
 */
public class BuiltInGraphParameters {

	private EnableOnConnectedSubgraphPort enableOnConnectedSubgraphPort;
	
	public BuiltInGraphParameters(TransformationGraph graph, GraphParameters parentGraphParameters) {
		this.enableOnConnectedSubgraphPort = new EnableOnConnectedSubgraphPort(graph, parentGraphParameters);
	}

	/**
	 * @return true if built-in graph parameter with given name exists
	 */
	public boolean hasGraphParameter(String name) {
		return enableOnConnectedSubgraphPort.hasGraphParameter(name);
	}

	/**
	 * @return built-in graph parameter with given name or null
	 */
	public GraphParameter getGraphParameter(String name) {
		for (GraphParameter graphParameter : getAllGraphParameters()) {
			if (graphParameter.getName().equals(name)) {
				return graphParameter;
			}
		}
		return null;
	}

	/**
	 * @return list of all built-in graph parameters
	 */
	public List<GraphParameter> getAllGraphParameters() {
		return enableOnConnectedSubgraphPort.getAllGraphParameters();
	}

	/**
	 * This inner class handle built-in graph parameters with following pattern:
	 * ENABLE_ON_INPUT_PORT_0_CONNECTED, DISABLE_ON_OUTPUT_PORT_1_CONNECTED, ...
	 * 
	 * NOTE: this is only implementation, we can create more similar class
	 * for other types of built-in graph parameters in the future
	 */
	public static class EnableOnConnectedSubgraphPort {
		private TransformationGraph graph;
		
		private GraphParameters parentGraphParameters;
		
		public EnableOnConnectedSubgraphPort(TransformationGraph graph, GraphParameters parentGraphParameters) {
			this.graph = graph;
			this.parentGraphParameters = parentGraphParameters;
		}
		
		public boolean hasGraphParameter(String name) {
			//graph parameters for input ports
			for (SubgraphPort inputPort : graph.getSubgraphInputPorts().getPorts()) {
				if (name.equals(getParameterName(true, true, inputPort.getIndex()))
						|| name.equals(getParameterName(false, true, inputPort.getIndex()))) {
					return true;
				}
			}
			//graph parameters for output ports
			for (SubgraphPort outputPort : graph.getSubgraphOutputPorts().getPorts()) {
				if (name.equals(getParameterName(true, false, outputPort.getIndex()))
						|| name.equals(getParameterName(false, false, outputPort.getIndex()))) {
					return true;
				}
			}
			return false;
		}

		public List<GraphParameter> getAllGraphParameters() {
			List<GraphParameter> result = new ArrayList<>();
			for (SubgraphPort inputPort : graph.getSubgraphInputPorts().getPorts()) {
				result.addAll(getGraphParametersForPort(inputPort));
			}
			for (SubgraphPort outputPort : graph.getSubgraphOutputPorts().getPorts()) {
				result.addAll(getGraphParametersForPort(outputPort));
			}
			return result;
		}

		private List<GraphParameter> getGraphParametersForPort(SubgraphPort port) {
			List<GraphParameter> result = new ArrayList<>();
			result.add(new GraphParameter(getParameterName(true, port.isInputPort(), port.getIndex()),
					Boolean.toString(port.isConnected()), parentGraphParameters));
			result.add(new GraphParameter(getParameterName(false, port.isInputPort(), port.getIndex()),
					Boolean.toString(!port.isConnected()), parentGraphParameters));
			return result;
		}
		
		/**
		 * Constructs name of built-in graph parameter.
		 * @param enable enable or disable on connected port
		 * @param inputPort input or output port
		 * @param portIndex index of the port
		 * @return name of built-in graph parameter with given specification
		 */
		public static String getParameterName(boolean enable, boolean inputPort, int portIndex) {
			return (enable ? "ENABLE" : "DISABLE") + "_ON_" + (inputPort ? "INPUT_PORT" : "OUTPUT_PORT") + "_" + portIndex + "_CONNECTED";
		}
	}
	
}
