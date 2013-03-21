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
package org.jetel.component.tree.reader.mappping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Common fields of mapping elements.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.12.2011
 */
public abstract class MappingElement {

	protected String xpath;
	protected Map<String, String> namespaceBinding = null;
	
	protected MappingElement parent;
	
	/**
	 * accepts visitor in breadth-first fashion
	 * @param visitor
	 */
	public abstract void acceptVisitor(MappingVisitor visitor);
	
	/**
	 * @return the parent
	 */
	public MappingElement getParent() {
		return parent;
	}
	
	/**
	 * @param parent the parent to set
	 */
	protected void setParent(MappingElement parent) {
		this.parent = parent;
	}
	
	public void addDefaultNamespace(String uri) {
		if (namespaceBinding == null) {
			namespaceBinding = new HashMap<String, String>(1);
		}
		namespaceBinding.put("", uri);
	}
	
	public void addNamespaceBinding(String prefix, String uri) {
		if (namespaceBinding == null) {
			namespaceBinding = new HashMap<String, String>(1);
		}
		namespaceBinding.put(prefix, uri);
	}
	
	/**
	 * @return the namespaceBinding
	 */
	public Map<String, String> getNamespaceBinding() {
		return namespaceBinding == null ? Collections.<String, String>emptyMap() : Collections.unmodifiableMap(namespaceBinding);
	}
	
	/**
	 * @return the xpath
	 */
	public String getXPath() {
		return xpath;
	}
	
	/**
	 * @param xpath the xpath to set
	 */
	public void setXPath(String xpath) {
		this.xpath = xpath;
	}
}
