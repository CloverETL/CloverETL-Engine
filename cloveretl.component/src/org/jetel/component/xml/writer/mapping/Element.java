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
package org.jetel.component.xml.writer.mapping;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.jetel.component.xml.writer.MappingVisitor;
import org.jetel.util.string.StringUtils;

/**
 * Class representing xml element. Can be treated as a template which can be referenced.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 14 Dec 2010
 */
public class Element extends AbstractElement {

	public static final boolean HIDE_DEFAULT = false;
	public static final boolean WRITE_NULL_DEFAULT = false;
	
	public static final MappingProperty[] AVAILABLE_PROPERTIES = {MappingProperty.NAME, 
		MappingProperty.WRITE_NULL_ELEMENT, MappingProperty.INPUT_PORT, MappingProperty.KEY, MappingProperty.PARENT_KEY,
		MappingProperty.HIDE, MappingProperty.FILTER, MappingProperty.PARTITION,
		MappingProperty.INCLUDE, MappingProperty.EXCLUDE, MappingProperty.WRITE_NULL_ATTRIBUTE, MappingProperty.OMIT_NULL_ATTRIBUTE};

	public static final MappingProperty[] AVAILABLE_PROPERTIES_SIMPLE = {MappingProperty.NAME, MappingProperty.VALUE,
		MappingProperty.WRITE_NULL_ELEMENT, MappingProperty.INPUT_PORT, MappingProperty.KEY, MappingProperty.PARENT_KEY,
		MappingProperty.HIDE, MappingProperty.FILTER, MappingProperty.PARTITION,
		MappingProperty.INCLUDE, MappingProperty.EXCLUDE, MappingProperty.WRITE_NULL_ATTRIBUTE, MappingProperty.OMIT_NULL_ATTRIBUTE};

	private Relation relation = null;
	private List<AbstractElement> children = new LinkedList<AbstractElement>();
	private List<Namespace> namespaces = new LinkedList<Namespace>();
	private WildcardElement wildcardAttribute = null;
	private List<Attribute> attributes = new LinkedList<Attribute>();

	private boolean template = false;

	
	public Element(Element parent) {
		super(parent, true);
	}

	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}
		
	public Element getRecurringParent() {
		if (parent != null) {
			if (parent.getRelation() != null) {
				return parent;
			} else {
				return parent.getRecurringParent();
			}
		}
		return null;
	}

	public void setAttribute(String localName, String value) throws XMLStreamException {
		MappingProperty property = MappingProperty.fromString(localName);
		if (!setProperty(property, value)) {
			throw new XMLStreamException(XmlMapping.UNKNOWN_ATTRIBUTE + localName);
		}
	}
	
	@Override
	public boolean setProperty(MappingProperty property, String value) {
		switch (property) {
		case NAME:
		case TEMPLATE_NAME:
		case WRITE_NULL_ELEMENT:
		case WRITE_NULL_ATTRIBUTE:
		case OMIT_NULL_ATTRIBUTE:
		case HIDE:
		case PARTITION:
			if (StringUtils.isEmpty(value)) {
				properties.remove(property);
			} else {
				properties.put(property, value);
			}
			break;

		case INPUT_PORT:
		case KEY:
		case PARENT_KEY:
		case FILTER:
			if (relation == null) {
				relation = new Relation(this);
			}
			relation.setProperty(property, value);
			break;

		case EXCLUDE:
		case INCLUDE:
			if (wildcardAttribute == null) {
				wildcardAttribute = new WildcardElement(this, false);
			}
			wildcardAttribute.setProperty(property, value);
			break;
			
		case VALUE:
			if (isSimple()) {
				if (children.size() == 1) {
					if (StringUtils.isEmpty(value)) {
						children.remove(0);
					} else {
						children.get(0).setProperty(MappingProperty.VALUE, value);
					}
				} else {
					if (!StringUtils.isEmpty(value)) {
						Value objectValue = new Value(this);
						objectValue.setProperty(MappingProperty.VALUE, value);
					}
				}
			}
			break;
			
		default:
			return false;
		}
		return true;
	}

	@Override
	public String getProperty(MappingProperty property) {
		switch (property) {
		case NAME:
		case TEMPLATE_NAME:
		case WRITE_NULL_ELEMENT:
		case WRITE_NULL_ATTRIBUTE:
		case OMIT_NULL_ATTRIBUTE:
		case HIDE:
		case PARTITION:
			return properties.get(property);

		case INPUT_PORT:
		case KEY:
		case PARENT_KEY:
		case FILTER:
			if (relation == null) {
				return null;
			}
			return relation.getProperty(property);

		case EXCLUDE:
		case INCLUDE:
			if (wildcardAttribute == null) {
				return null;
			}
			return wildcardAttribute.getProperty(property);
			
		case VALUE:
			if (isSimple() && children.size() == 1) {
				return children.get(0).getProperty(MappingProperty.VALUE);
			} else {
				return null;
			}
			
		default:
			return super.getProperty(property);
		}
	}

	@Override
	public MappingProperty[] getAvailableProperties() {
		if (isSimple()) {
			return AVAILABLE_PROPERTIES_SIMPLE; 
		}
		return AVAILABLE_PROPERTIES;
	}
	
	public void addAttribute(int position, String name, String value) {
		Attribute attribute = new Attribute(this);
		attribute.setProperty(MappingProperty.NAME, name);
		attribute.setProperty(MappingProperty.VALUE, value);
		attributes.add(position, attribute);
	}
	
	public void addAttribute(int position, Attribute attribute) {
		attributes.add(position, attribute);
	}
	
	public void removeAttribute(int position) {
		attributes.remove(position);
	}

	public void addNamespace(int position, String prefix, String URI) {
		Namespace namespace = new Namespace(this);
		namespace.setProperty(MappingProperty.NAME, prefix);
		namespace.setProperty(MappingProperty.VALUE, URI);
		namespaces.add(position, namespace);
	}
	
	public void addNamespace(int position, Namespace namespace) {
		namespaces.add(position, namespace);
	}
	
	public void removeNamespace(int position) {
		namespaces.remove(position);
	}

	public void addChild(int position, AbstractElement value) {
		children.add(position, value);
	}
	
	public void removeChild(int position) {
		children.remove(position);
	}

	public List<AbstractElement> getChildren() {
		return Collections.unmodifiableList(children);
	}

	public void setChildren(List<AbstractElement> children) {
		this.children = children;
	}

	public List<Attribute> getAttributes() {
		return Collections.unmodifiableList(attributes);
	}

	public List<Namespace> getNamespaces() {
		return Collections.unmodifiableList(namespaces);
	}

	public Relation getRelation() {
		return relation;
	}

	public void setRelation(Relation recurringInfo) {
		this.relation = recurringInfo;
	}

	public WildcardElement getWildcardAttribute() {
		return wildcardAttribute;
	}

	public void setWildcardAttribute(WildcardElement wildcardAttribute) {
		this.wildcardAttribute = wildcardAttribute;
	}

	public boolean isTemplate() {
		return template;
	}

	public void setTemplate(boolean template) {
		this.template = template;
	}

	public boolean isPartition() {
		return Boolean.parseBoolean(getProperty(MappingProperty.PARTITION));
	}

	@Override
	public String toString() {
		return (relation == null ? "static" : "recurring") + " ObjectElement [name=" 
			+ properties.get(MappingProperty.NAME) + "]";
	}

	@Override
	public String getSimpleContent() {
		if (isSimple() && children.size() == 1){
			return children.get(0).getSimpleContent();
		}
		
		return null;
	}

	@Override
	public String getDisplayName() {
		return template ? properties.get(MappingProperty.TEMPLATE_NAME) : properties.get(MappingProperty.NAME);
	}
	
	public boolean isSimple() {
		return relation == null && namespaces.isEmpty() && wildcardAttribute == null && attributes.isEmpty()
			&& (children.isEmpty() || (children.size() == 1 && children.get(0).getType() == AbstractElement.VALUE));
	}
	
	public String getPath() {
		if (parent == null) {
			return ""; 
		} else {
			if (relation != null && relation.getProperty(MappingProperty.INPUT_PORT) != null) {
				return super.getPath() + " [$" + relation.getProperty(MappingProperty.INPUT_PORT) + "]";
			}
			return super.getPath();
		}
	}

	@Override
	public short getType() {
		return template ? AbstractElement.TEMPLATE : AbstractElement.ELEMENT;
	}

	@Override
	public String getDescription() {
		return template ? "Template declaration" : "An XML element. Example: <element0>";
	}
}
