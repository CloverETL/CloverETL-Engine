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
import java.util.Map.Entry;



/**  Represents a mapping from an XML structure to a clover record.
 *  Describes how a data (be it attributes, nested elements or text value) from given XML element are mapped to a 
 *  fields of clover record.  
 * 
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.6.2012
 */
public class XMLMappingsDefinition extends XMLMappingDefinition {
	/**
	 * Map associating the element path with a type override definition.  
	 */
	private Map<String, TypeOverrideDefinition> typeOverrides = new HashMap<String, TypeOverrideDefinition>();

	public Map<String, TypeOverrideDefinition> getTypeOverrides() {
		return typeOverrides;
	}

	public void setTypeOverrides(Map<String, TypeOverrideDefinition> typeOverrides) {
		this.typeOverrides = typeOverrides;
	}

	/** Copies the attributes of this mapping to a given mapping.
	 * 
	 * @param mapping
	 */
	public void copyTo(XMLMappingsDefinition mapping) {
		Map<String, TypeOverrideDefinition> typeOverridesCopy = new HashMap<String, TypeOverrideDefinition>();
		for (Entry<String, TypeOverrideDefinition> entry : typeOverridesCopy.entrySet()) {
			typeOverridesCopy.put(entry.toString(), entry.getValue().createCopy());
		}
		
		super.copyTo(mapping);
	}	
	
	@Override
	public XMLMappingsDefinition createCopy() {
		XMLMappingsDefinition mappingsDefinition = new XMLMappingsDefinition();
		
		copyTo(mappingsDefinition);
		
		return mappingsDefinition;
	}
}
