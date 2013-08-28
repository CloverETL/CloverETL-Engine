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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.graph.runtime.IAuthorityProxy;

/**
 * This class represents single graph parameter - name-value pair, which is
 * used to resolve all ${PARAM_NAME} references in grf files.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.8.2013
 */
@XmlRootElement(name = "GraphParameter")
@XmlType(propOrder = { "description", "secure", "value", "name" })
public class GraphParameter {

	private String name;
	
	private String value;
	
	private boolean secure;
	
	private String description;
	
	public GraphParameter() {
		
	}
	
	public GraphParameter(String name, String value) {
		this.name = name;
		this.value = value;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * @return name of this graph parameter, this is key of parameter
	 * which is used to reference this parameter using ${PARAM_NAME} pattern
	 */
	@XmlAttribute(name="name")
	public String getName() {
		return name;
	}
	
	public void setValue(String value) {
		this.value = value;
	}
	
	/**
	 * @return value of this parameter; can contain a parameter reference recursively
	 */
	@XmlAttribute(name="value")
	public String getValue() {
		return value;
	}
	
	/**
	 * @return true if this parameter is considered as secured;
	 * special value resolution is used for secure parameters,
	 * see {@link IAuthorityProxy#getSecureParamater(String, String)}
	 */
	@XmlAttribute(name="secure")
	public boolean isSecure() {
		return secure;
	}
	
	/**
	 * Marks this parameter as secure parameter.
	 * @param secure
	 */
	public void setSecure(boolean secure) {
		this.secure = secure;
	}

	/**
	 * @return description of this graph parameter
	 * @note this attribute is not de-serialize from xml now by TransformationGraphXMLReaderWriter
	 */
	@XmlAttribute(name="description")
	public String getDescription() {
		return description;
	}

	/**
	 * Sets description of this graph parameter
	 * @param description new description of this graph parameter
	 */
	public void setDescription(String description) {
		this.description = description;
	}

}
