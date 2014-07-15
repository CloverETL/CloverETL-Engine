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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.ComponentDescription.Attribute;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.parameter.GraphParameterAttributeNode;
import org.jetel.graph.parameter.GraphParameterDynamicValueProvider;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.util.SubgraphUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
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
@XmlType(propOrder = { "name", "value", "secure", "componentReference", "attrs", "singleType" })
public class GraphParameter {

	public static final String HIDDEN_SECURE_PARAMETER = "*****";
	
	private static final SingleType DEFAULT_SINGLE_TYPE = new SingleType("string");
	
	private String name;
	
	private String value;
	
	private GraphParameterDynamicValueProvider dynamicValue;
	
	private String label;
	
	private boolean secure = false;
	
	private boolean isPublic = false;
	
	private boolean isRequired = false;
	
	private String description;
	
	private String category;

	private String defaultHint;

	private SingleType singleType;

	private ComponentReference componentReference;
	
	@XmlTransient
	private GraphParameters parentGraphParameters;
	
	public GraphParameter() {
		
	}
	
	public GraphParameter(String name, String value) {
		this.name = name;
		this.value = value;
	}

	GraphParameter(String name, String value, GraphParameters parentGraphParameters) {
		this.name = name;
		this.value = value;
		this.parentGraphParameters = parentGraphParameters;
	}

	void setParentGraphParameters(GraphParameters parentGraphParameters) {
		this.parentGraphParameters = parentGraphParameters;
	}
	
	@XmlTransient
	public TransformationGraph getParentGraph() {
		return parentGraphParameters != null ? parentGraphParameters.getParentGraph() : null;
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
		if (value != null) {
			this.dynamicValue = null; 
		}
	}
	
	/**
	 * @return true if value of this graph parameter is defined by CTL code
	 */
	public boolean hasDynamicValue() {
		return dynamicValue != null;
	}
	
	/**
	 * @param flag 
	 * @return resolved value of this graph parameter
	 * @see PropertyRefResolver
	 */
	public String getValueResolved(RefResFlag flag) {
		if (!hasDynamicValue()) {
			return getParentGraph().getPropertyRefResolver().resolveRef(value, flag);
		} else {
			return dynamicValue.getValue();
		}
	}
	
	/**
	 * @return value of this parameter; can contain a parameter reference recursively
	 */
	@XmlAttribute(name="value")
	public String getValue() {
		if (!hasDynamicValue()) {
			return value != null ? value : "";
		} else {
			return dynamicValue.getValue();
		}
	}
	
	public GraphParameterAttributeNode[] getAttrs() {
		List<GraphParameterAttributeNode> ret = new ArrayList<>();

		if (dynamicValue != null) {
			GraphParameterAttributeNode attrNode = new GraphParameterAttributeNode();
			attrNode.setName("dynamicValue");
			attrNode.setValue(dynamicValue.getTransformCode());
			ret.add(attrNode);
		}

		if (description != null) {
			GraphParameterAttributeNode attrNode = new GraphParameterAttributeNode();
			attrNode.setName("description");
			attrNode.setValue(description);
			ret.add(attrNode);
		}

		return ret.toArray(new GraphParameterAttributeNode[ret.size()]);
	}

	@XmlElement(name="attr")
	public void setAttrs(GraphParameterAttributeNode[] attrs) {
		for (GraphParameterAttributeNode a : attrs) {
			if ("dynamicValue".equals(a.getName())) {
				setDynamicValue(a.getValue());
			}
			if ("description".equals(a.getName())) {
				setDescription(a.getValue());
			}
		}
	}

	public void setDynamicValue(String dynamicValue) {
		if (dynamicValue != null && !dynamicValue.isEmpty()) {
			this.dynamicValue = GraphParameterDynamicValueProvider.create(this, dynamicValue);
		}
		else {
			this.dynamicValue = null;
		}
	}
	
	@XmlTransient
	public String getDynamicValue() {
		if (this.dynamicValue != null) {
			return dynamicValue.getTransformCode();
		}
		else {
			return null;
		}
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
	@XmlTransient
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
	
	@XmlAttribute(name="category")
	public String getCategory() {
		return category;
	}
	
	public void setCategory(String category) {
		this.category = category;
	}

	@XmlAttribute(name="defaultHint")
	public String getDefaultHint() {
		return defaultHint;
	}
	
	public void setDefaultHint(String defaultHint) {
		this.defaultHint = defaultHint;
	}

	@XmlElement(name="SingleType")
	public SingleType getSingleType() {
		return singleType;
	}

	/**
	 * Returns single type of this graph parameter. Reference type definition is recursively resolved.
	 * If no single type is defined, 'string' type is returned as default.
	 */
	public SingleType getSingleTypeRecursive() {
		if (singleType != null) {
			//single type defined
			return singleType;
		} else if (componentReference != null) {
			//type is defined by component reference
			if (getParentGraph() != null) {
				//graph is available
				Node component = getParentGraph().getNodes().get(componentReference.getComponentId());
				if (component != null) {
					//referenced component is available
					if (!SubgraphUtils.isSubJobComponent(component.getType())) {
						//referenced component is regular component
						Attribute attribute = component.getDescriptor().getDescription().getAttributes().getAttribute(componentReference.getAttributeName());
						if (attribute != null) {
							//referenced attribute found
							if (attribute.getSingleType() != null) {
								//referenced attribute is single type - let's return this single type
								return new SingleType(attribute.getSingleType().getName());
							} else {
								throw new JetelRuntimeException("Graph parameter '" + getName() + "' references non-single type attribute '" + componentReference + "'.");
							}
						} else {
							throw new JetelRuntimeException("Graph parameter '" + getName() + "' references unknown attribute '" + componentReference + "'.");
						}
					} else {
						//referenced component is Subgraph component
						SubgraphComponent subgraphComponent = (SubgraphComponent) component;
						TransformationGraph subgraph = subgraphComponent.getSubgraphNoMetadataPropagation(true);
						String graphParameterName = SubgraphUtils.getPublicGraphParameterName(componentReference.getAttributeName());
						if (subgraph.getGraphParameters().hasGraphParameter(graphParameterName)) {
							//referenced public subgraph parameter found
							GraphParameter subgraphGraphParameter = subgraph.getGraphParameters().getGraphParameter(graphParameterName);
							try {
								//recursive search of single type
								return subgraphGraphParameter.getSingleTypeRecursive();
							} catch (Exception e) {
								throw new JetelRuntimeException("Graph parameter '" + getName() + "' reference resolution failed (" + componentReference + ").");
							}
						} else {
							throw new JetelRuntimeException("Graph parameter '" + getName() + "' references unknown attribute '" + componentReference + "'.");
						}
					}
				} else {
					throw new JetelRuntimeException("Graph parameter '" + getName() + "' references unknown component '" + componentReference.getComponentId() + "'.");
				}
			} else {
				throw new JetelRuntimeException("Graph parameter '" + getName() + "' cannot provide referenced type. Unknown parent graph.");
			}
		} else {
			return DEFAULT_SINGLE_TYPE;
		}
	}
	
	public void setSingleType(SingleType singleType) {
		this.singleType = singleType;
	}

	public void setSingleType(String singleTypeName) {
		this.singleType = new SingleType(singleTypeName);
	}

	@XmlElement(name="ComponentReference")
	public ComponentReference getComponentReference() {
		return componentReference;
	}

	public void setComponentReference(ComponentReference componentReference) {
		this.componentReference = componentReference;
	}

	public void setComponentReference(String referencedComponentId, String referencedAttributeName) {
		this.componentReference = new ComponentReference(referencedComponentId, referencedAttributeName);
	}

	@XmlType
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
	
	@XmlType(propOrder = { "componentId", "attributeName" })
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
		
		@Override
		public String toString() {
			return componentId + "." + attributeName;
		}
	}
	
}
