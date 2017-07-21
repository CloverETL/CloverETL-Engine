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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.CloverPublicAPI;
import org.jetel.util.CompareUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.string.StringUtils;

/**
 * Container for all graph parameters associated with a transformation graph.
 * @see TransformationGraph#getGraphParameters()
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.8.2013
 */
@XmlRootElement(name = "GraphParameters")
@CloverPublicAPI
public class GraphParameters {

	/** User's graph parameters. Access must be synchronized to avoid ConcurrentModificationException. 
	 * SynchronizedMap wouldn't be sufficient, since it doesn't synchronize iteration.  */
	private final Map<String, GraphParameter> userParameters = new LinkedHashMap<String, GraphParameter>();
	
	/**
	 * This class handles built-in graph parameters.
	 */
	private BuiltInGraphParameters builtInParameters;
	
	@XmlTransient
	private TransformationGraph parentGraph;
	
	public GraphParameters() {
	}

	public GraphParameters(TransformationGraph parentGraph) {
		this.parentGraph = parentGraph;
		builtInParameters = new BuiltInGraphParameters(parentGraph, this);
	}
	
	public GraphParameters(Properties properties) {
		addProperties(properties);
	}

	/**
	 * @return parent transformation graph or null
	 */
	@XmlTransient
	public TransformationGraph getParentGraph() {
		return parentGraph;
	}
	
	/**
	 * Both user's and built-in graph parameters are considered.
	 * @param name name of searched parameter
	 * @return true if parameter with given name is in this parameters container
	 */
	public boolean hasGraphParameter(String name) {
		synchronized (userParameters) {
			return userParameters.containsKey(name)
					|| (builtInParameters != null && builtInParameters.hasGraphParameter(name));
		}
	}
	
	/**
	 * Both user's and built-in graph parameters are considered.
	 * @param name
	 * @return {@link GraphParameter} for given parameter name;
	 * if no parameter with the given name is part of this container,
	 * dummy graph parameter is returned anyway with null value
	 */
	public GraphParameter getGraphParameter(String name) {
		synchronized (userParameters) {
			GraphParameter result = userParameters.get(name);
			if (result != null) {
				return result;
			} else {
				if (builtInParameters != null && builtInParameters.hasGraphParameter(name)) {
					return builtInParameters.getGraphParameter(name);
				} else {
					return new GraphParameter(name, null, this);
				}
			}
		}
	}
	
	/**
	 * @return list of all graph parameters from this container (only user's graph parameters are considered)
	 */
	@XmlElement(name = "GraphParameter")
	public List<GraphParameter> getAllGraphParameters() {
		synchronized (userParameters) {
			return new ArrayList<GraphParameter>(userParameters.values());
		}
	}
	
	/**
	 * @return list of all built-in parameters
	 */
	public List<GraphParameter> getAllBuiltInParameters() {
		synchronized (userParameters) {
			if (builtInParameters != null) {
				return builtInParameters.getAllGraphParameters();
			} else {
				return new ArrayList<>();
			}
		}
	}
	
	/**
	 * Adds new graph parameter to this container.
	 * @param name name of the new parameter
	 * @param value value of the new parameter
	 * @return created {@link GraphParameter} instance
	 */
	public GraphParameter addGraphParameter(String name, String value) {
		GraphParameter graphParameter = new GraphParameter(name, value);
		if (this.parentGraph != null) {
			this.parentGraph.getAuthorityProxy().modifyGraphParameter(graphParameter);
		}
		addGraphParameter(graphParameter);
		return getGraphParameter(name);
	}

	/**
	 * Adds the given graph parameter to this container.
	 * Name of the given parameter cannot be empty.
	 * @param graphParameter
	 * @return true if parameter has been successfully added to this container,
	 * false if parameter name is already engaged
	 */
	public boolean addGraphParameter(GraphParameter graphParameter) {
		if (StringUtils.isEmpty(graphParameter.getName())) {
			throw new JetelRuntimeException("empty graph paramater name");
		}
		synchronized (userParameters) {
			if (!userParameters.containsKey(graphParameter.getName())) {
				graphParameter.setParentGraphParameters(this);
				userParameters.put(graphParameter.getName(), graphParameter);
				return true;
			} else {
				return false;
			}
		}
	}

	/**
	 * Bulk operation add to this container.
	 * @param graphParameters list of graph parameters to be added into this container
	 * @see #addGraphParameter(GraphParameter)
	 */
	public void addGraphParameters(List<GraphParameter> graphParameters) {
		for (GraphParameter graphParameter : graphParameters) {
			addGraphParameter(graphParameter);
		}
	}
	
	/**
	 * Clears content of this graph parameters and adds new graph parameters
	 * specified by given properties.
	 * @param properties
	 */
	public void setProperties(Properties properties) {
		synchronized (userParameters) {
			userParameters.clear();
			addProperties(properties);
		}
	}
	
	/**
	 * Adds new graph parameters specified by given properties.
	 * @param properties
	 */
	public void addProperties(Properties properties) {
		if (properties != null) {
			for (String propertyName : properties.stringPropertyNames()) {
				addGraphParameter(propertyName, properties.getProperty(propertyName));
			}
		}
	}

	/**
	 * Adds new graph parameters based on give properties. Values of existing
	 * parameters are overridden.
	 * @param properties
	 * @param canBeResolved flag indicates the graph parameter values should be resolved or not
	 * (public graph parameters of subgraph passed from parent graph should not be resolved, see CLO-7110) 
	 */
	public void addPropertiesOverride(Properties properties, boolean canBeResolved) {
		if (properties != null) {
			for (String propertyName : properties.stringPropertyNames()) {
				GraphParameter gp;
				if (hasGraphParameter(propertyName)) {
					gp = getGraphParameter(propertyName);
					gp.setValue(properties.getProperty(propertyName));
				} else {
					gp = addGraphParameter(propertyName, properties.getProperty(propertyName));
				}
				gp.setCanBeResolved(canBeResolved);
			}
		}
	}

	/**
	 * @return content of this container in properties form (only user's graph parameters are considered)
	 */
	public TypedProperties asProperties() {
		TypedProperties result = new TypedProperties();
		
		synchronized (userParameters) {
			for (GraphParameter parameter : userParameters.values()) {
				result.setProperty(parameter.getName(), parameter.getValue());
			}
		}
		
		return result;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		synchronized (userParameters) {
			List<GraphParameter> orderedParams = new ArrayList<>(userParameters.values());
			Collections.sort(orderedParams, new Comparator<GraphParameter>() {
				@Override
				public int compare(GraphParameter p1, GraphParameter p2) {
					return CompareUtils.compare(p1.getName(), p2.getName());
				}
			});
			for (Iterator<GraphParameter> it = orderedParams.iterator(); it.hasNext();) {
				GraphParameter parameter = it.next();
				result.append(parameter.getName());
				result.append('=');
				if (parameter.isSecure()) {
					result.append(GraphParameter.HIDDEN_SECURE_PARAMETER);
				} else {
					result.append(parameter.getValue());
				}
				if (it.hasNext()) {
					result.append('\n');
				}
			}
		}
		return result.toString();
	}
	
	/**
	 * Clears this graph parameters.
	 */
	public void clear() {
		synchronized (userParameters) {
			List<GraphParameter> oldGraphParameters = getAllGraphParameters();
			userParameters.clear();
			//clear references to parent GraphParameters
			for (GraphParameter oldGraphParameter : oldGraphParameters) {
				oldGraphParameter.setParentGraphParameters(null);
			}
		}
	}
	
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		synchronized (userParameters) {
			for (GraphParameter param: userParameters.values()) {
				if (!StringUtils.isValidObjectName(param.getName())) {
					status.add("Invalid name of graph parameter: '" + param.getName() + "'", Severity.ERROR, null, Priority.HIGH);
				}
			}
		}
		return status;
	}

}
