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
package org.jetel.graph.parameter;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

/**
 * Graph parameter attribute node.
 * 
 * <p>For storing graph parameter attributes not as XML attributes, but as child elements.
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1. 5. 2014
 */
@XmlRootElement(name="attr")
@XmlType(propOrder = { "name" })
public class GraphParameterAttributeNode {
	
	private String name;
	private String value;
	
	public GraphParameterAttributeNode() {
	}
	
	/**
	 * @param name the name of the attribute
	 * @param value the value of the attribute
	 */
	public GraphParameterAttributeNode(String name, String value) {
		super();
		this.name = name;
		this.value = value;
	}

	/**
	 * @return the name of the attribute
	 */
	@XmlAttribute(name="name")
	public String getName() {
		return name;
	}
	
	/**
	 * @param name the name of the attribute
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * @return the value of the attribute
	 */
	@XmlValue
	public String getValue() {
		return value;
	}
	
	/**
	 * @param value the value of the attribute
	 */
	public void setValue(String value) {
		this.value = value;
	}
	
}