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



/**  Represents a type override.
 * 
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.6.2012
 */
public class TypeOverrideDefinition extends XMLMappingDefinition {
	/**
	 * XML element path.
	 */
	private String elementPath; 

	/**
	 * Type that should be used for the element.
	 */
	private String overridingType; 
	
	/**
	 * Creates a new instance of the element to record mapping as root.
	 */
	public TypeOverrideDefinition() {
		super();
	}

	/**
	 * Creates a new instance of the element to record mapping with given parent.
	 * 
	 * @param parent
	 */
	public TypeOverrideDefinition(XMLMappingDefinition parent) {
		super(parent);
	}

	/** Copies an attributes of this mapping to a given mapping.
	 * 
	 * @param mapping
	 */
	public void copyTo(TypeOverrideDefinition mapping) {
		super.copyTo(mapping);
		
		mapping.setElementPath(elementPath);
		mapping.setOverridingType(overridingType);
	}

	@Override
	public TypeOverrideDefinition createCopy() {
		TypeOverrideDefinition mapping = new TypeOverrideDefinition();
		
		this.copyTo(mapping);
		
		return mapping;
	}

	public String getElementPath() {
		return elementPath;
	}

	public void setElementPath(String elementPath) {
		this.elementPath = elementPath;
	}

	public String getOverridingType() {
		return overridingType;
	}

	public void setOverridingType(String overridingType) {
		this.overridingType = overridingType;
	}	
}
