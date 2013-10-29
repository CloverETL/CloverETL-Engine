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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.exception.JetelRuntimeException;
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
public class GraphParameters {

	private final Map<String, GraphParameter> parameters = new LinkedHashMap<String, GraphParameter>();
	
	public GraphParameters() {
	}

	public GraphParameters(Properties properties) {
		addProperties(properties);
	}

	/**
	 * @param name name of searched parameter
	 * @return true if parameter with given name is in this parameters container
	 */
	public boolean hasGraphParameter(String name) {
		return parameters.containsKey(name);
	}
	
	/**
	 * @param name
	 * @return {@link GraphParameter} for given parameter name;
	 * if no parameter with the given name is part of this container,
	 * dummy graph parameter is returned anyway with null value
	 */
	public GraphParameter getGraphParameter(String name) {
		GraphParameter result = parameters.get(name);
		if (result != null) {
			return result;
		} else {
			return new GraphParameter(name, null);
		}
	}
	
	/**
	 * @return list of all graph parameters from this container
	 */
	@XmlElement(name = "GraphParameter")
	public List<GraphParameter> getAllGraphParameters() {
		return new ArrayList<GraphParameter>(parameters.values());
	}
	
	/**
	 * Adds new graph parameter to this container.
	 * @param name name of the new parameter
	 * @param value value of the new parameter
	 * @return created {@link GraphParameter} instance
	 */
	public GraphParameter addGraphParameter(String name, String value) {
		GraphParameter graphParameter = new GraphParameter(name, value);
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
		if (!parameters.containsKey(graphParameter.getName())) {
			parameters.put(graphParameter.getName(), graphParameter);
			return true;
		} else {
			return false;
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
		parameters.clear();
		addProperties(properties);
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
	 */
	public void addPropertiesOverride(Properties properties) {
		if (properties != null) {
			for (String propertyName : properties.stringPropertyNames()) {
				if (hasGraphParameter(propertyName)) {
					getGraphParameter(propertyName).setValue(properties.getProperty(propertyName));
				} else {
					addGraphParameter(propertyName, properties.getProperty(propertyName));
				}
			}
		}
	}

	/**
	 * @return content of this container in properties form
	 */
	public TypedProperties asProperties() {
		TypedProperties result = new TypedProperties();
		
		for (GraphParameter parameter : parameters.values()) {
			result.setProperty(parameter.getName(), parameter.getValue());
		}
		
		return result;
	}

	/**
	 * Clears this graph parameters.
	 */
	public void clear() {
		parameters.clear();
	}
	
}
