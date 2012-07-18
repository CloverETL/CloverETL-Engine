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
package org.jetel.data.xml.mapping;

import java.util.HashMap;
import java.util.Map;

import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.AutoFilling;

/** Represents a context in which the mapping should work.
 * 
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.6.2012
 */
public class XMLMappingContext {
	/**
	 * Graph context.
	 */
    private TransformationGraph graph;
    
    /**
     * Component context.
     */
    private Node parentComponent;
    
	/**
	 * Map associating prefix with a namespace URI.
	 */
	private Map<String, String> namespaceBindings = new HashMap<String, String>();

	/**
	 * Autofilling to be used.
	 */
	private AutoFilling autoFilling = new AutoFilling();
	
	/**
	 * Creates new instance 
	 */
	public XMLMappingContext() {
		super();
	}

	
	
	public TransformationGraph getGraph() {
		return graph;
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public Node getParentComponent() {
		return parentComponent;
	}

	public void setParentComponent(Node parentComponent) {
		this.parentComponent = parentComponent;
	}

	public Map<String, String> getNamespaceBindings() {
		return namespaceBindings;
	}

	public void setNamespaceBindings(Map<String, String> namespaceBindings) {
		this.namespaceBindings = namespaceBindings;
	}

	public AutoFilling getAutoFilling() {
		return autoFilling;
	}

	public void setAutoFilling(AutoFilling autoFilling) {
		this.autoFilling = autoFilling;
	}
	
}
