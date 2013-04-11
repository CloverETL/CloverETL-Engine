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

import java.util.Iterator;
import java.util.Map;

import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.lookup.LookupTable;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.property.PropertyRefResolver;

/**
 * Implementation of {@link GraphWrapper} for graph from engine.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 25.3.2013
 */
public class EngineGraphWrapper implements GraphWrapper {
	private TransformationGraph graph;
	private PropertyRefResolver refResolver;
	private Map<ValidationNode, ValidationGroup> parentTable;
	
	public EngineGraphWrapper(TransformationGraph graph) {
		this.graph = graph;
		this.refResolver = new PropertyRefResolver(graph.getGraphProperties());
	}
	
	public Iterator<String> getLookupTables() {
		return graph.getLookupTables();
	}
	
	public LookupTable getLookupTable(String name) {
		return graph.getLookupTable(name);
	}
	
	public PropertyRefResolver getRefResolver() {
		return refResolver;
	}
	
	public void initParentTable(ValidationGroup root) {
		parentTable = ValidatorUtils.createParentTable(root);
	}
	
	public String getNodePath(ValidationNode needle) {
		return ValidatorUtils.getNodePath(needle, parentTable, ";");
	}
}
