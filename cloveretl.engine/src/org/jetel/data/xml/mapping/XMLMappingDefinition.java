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

import java.util.LinkedList;
import java.util.List;

/** Represents a mapping from an XML structure to a clover record.
 * 
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.6.2012
 */
public abstract class XMLMappingDefinition {
	/**
	 * Parent mapping. If <code>null</code>, this mapping is assumed to be a root mapping. 
	 */
	private XMLMappingDefinition parent;

	/**
	 * Child mappings.
	 */
	private List<XMLMappingDefinition> children = new LinkedList<XMLMappingDefinition>();
	
	/** 
	 *  Creates new instance of the mapping with a given parent.
	 *  
	 *  @param parent
	 */
	public XMLMappingDefinition(XMLMappingDefinition parent) {
		this.parent = parent;
	}

	/** Copies a fields from this mapping to given mapping.
	 * 
	 * @param mapping
	 */
	public void copyTo(XMLMappingDefinition mapping) {
		mapping.setParent(parent);
		
		mapping.getChildren().clear();
		
		for (XMLMappingDefinition child : children) {
			mapping.getChildren().add(child.createCopy());
		}
	}
	
	/** Creates a copy of this mapping.
	 * 
	 * @return a copy of this mapping.
	 */
	public abstract XMLMappingDefinition createCopy();
	
	/**
	 *  Creates a new instance of the mapping as root.
	 */
	public XMLMappingDefinition() {
		this(null);
	}

	public XMLMappingDefinition getParent() {
		return parent;
	}

	public void setParent(XMLMappingDefinition parent) {
		this.parent = parent;
	}

	public List<XMLMappingDefinition> getChildren() {
		return children;
	}

	public void setChildren(List<XMLMappingDefinition> children) {
		this.children = children;
	}
}
