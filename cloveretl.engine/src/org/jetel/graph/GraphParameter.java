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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.util.SubgraphUtils;
import org.jetel.util.string.StringUtils;

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
@XmlType(propOrder = { "name", "value", "secure", "description", "componentReference", "singleType" })
public class GraphParameter {

	public static final String HIDDEN_SECURE_PARAMETER = "*****";
	
	private String name;
	
	private String value;
	
	private String label;
	
	private boolean secure = false;
	
	private boolean isPublic = false;
	
	private boolean isRequired = false;
	
	private String description;
	
	private SingleType singleType;

	private ComponentReference componentReference;
	
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
		this.value = (value != null) ? value : "";
	}
	
	/**
	 * @return value of this parameter; can contain a parameter reference recursively
	 */
	@XmlAttribute(name="value")
	public String getValue() {
		return value;
	}
	
	/**
	 * Human-readable name of the parameter. Used for public subgraph parameters.
	 * @return the label
	 */
	@XmlAttribute(name="label")
	public String getLabel() {
		return label;
	}
	
	/**
	 * @return label or name if label is empty
	 */
	public String getLabelOrName() {
		if (!StringUtils.isEmpty(label)) {
			return label;
		} else {
			return name;
		}
	}
	
	/**
	 * @param label the label to set
	 */
	public void setLabel(String label) {
		this.label = label;
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
	 * @return true if this parameter is public.
	 */
	@XmlAttribute(name="public")
	public boolean isPublic() {
		return isPublic;
	}
	
	/**
	 * Marks this parameter as public parameter.
	 * @param isPublic the isPublic to set
	 */
	public void setPublic(boolean isPublic) {
		this.isPublic = isPublic;
	}

	/**
	 * @return true if this parameter is required.
	 */
	@XmlAttribute(name="required")
	public boolean isRequired() {
		return isRequired;
	}
	
	/**
	 * Marks this parameter as required parameter.
	 * @param isRequired the isRequired to set
	 */
	public void setRequired(boolean isRequired) {
		this.isRequired = isRequired;
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

	@XmlElement(name="singleType")
	public SingleType getSingleType() {
		return singleType;
	}

//	public SingleType getSingleTypeRecursive(TransformationGraph graph) {
//		if (singleType != null) {
//			return singleType;
//		} else if (componentReference != null) {
//			Node component = graph.getNodes().get(componentReference.getComponentId());
//			if (component != null) {
//				if (!SubgraphUtils.isSubJobComponent(component.getType())) {
//					component.getDescriptor().getDescription().getAttributes().get
//				} else {
//					
//				}
//			} else {
//				
//			}
//		}
//	}
	
	public void setSingleType(SingleType singleType) {
		this.singleType = singleType;
	}

	public void setSingleType(String singleTypeName) {
		this.singleType = new SingleType(singleTypeName);
	}

	@XmlElement(name="componentReference")
	public ComponentReference getComponentReference() {
		return componentReference;
	}

	public void setComponentReference(ComponentReference componentReference) {
		this.componentReference = componentReference;
	}

	public void setComponentReference(String referencedComponentId, String referencedAttributeName) {
		this.componentReference = new ComponentReference(referencedComponentId, referencedAttributeName);
	}

	public static class SingleType {
		private String name;

		public SingleType() {
		}

		public SingleType(String name) {
			this.name = name;
		}
		
		@XmlAttribute(name="name")
		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
	
	@XmlType(propOrder = { "referencedComponentId", "referencedAttributeName" })
	public static class ComponentReference {
		private String componentId;
		private String attributeName;

		public ComponentReference() {
		}

		public ComponentReference(String componentId, String attributeName) {
			this.componentId = componentId;
			this.attributeName = attributeName;
		}
		
		@XmlAttribute(name="referencedComponent")
		public String getComponentId() {
			return componentId;
		}

		public void setComponentId(String componentId) {
			this.componentId = componentId;
		}

		@XmlAttribute(name="referencedProperty")
		public String getAttributeName() {
			return attributeName;
		}

		public void setAttributeName(String attributeName) {
			this.attributeName = attributeName;
		}
	}
	
}
