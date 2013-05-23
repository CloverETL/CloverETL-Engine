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
package org.jetel.component.validator;

import java.util.List;
import java.util.Map;

import org.jetel.data.lookup.LookupTable;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.property.PropertyRefResolver;

/**
 * Wraps different implementation of graph to same functionality both in GUI and engine.
 * Also adds support for queries on validation tree during validation (look up rules and paths in three).
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 24.3.2013
 * @see EngineGraphWrapper
 */
public interface GraphWrapper {

	/**
	 * Returns names of all graphs lookup tables
	 * @return List of lookup tables name
	 */
	public List<String> getLookupTables();
	
	/**
	 * Returns lookup table with given name
	 * @param name Name of lookup table
	 * @return Lookup table with given name
	 */
	public LookupTable getLookupTable(String name);
	
	/**
	 * Returns property resolver
	 * @return Property resolver
	 */
	public PropertyRefResolver getRefResolver();
	
	/**
	 * Populate this wrapper with component validation group
	 * @param group Currently used validation group
	 */
	public void init(ValidationGroup group);
	
	/**
	 * Returns whole path to validation node
	 * 
	 * Call after {@link #init(ValidationGroup)}
	 * @param needle Validation node to find path to
	 * @return List of names of validation groups ending with needle name
	 */
	public List<String> getNodePath(ValidationNode needle);
	
	/**
	 * Returns all custom rules associated with current validation group.
	 * @return All custom rules
	 */
	public Map<Integer, CustomRule> getCustomRules();
	
	/**
	 * Return transformation (engine) graph
	 * @return Transformation graph
	 */
	public TransformationGraph getTransformationGraph();
}
