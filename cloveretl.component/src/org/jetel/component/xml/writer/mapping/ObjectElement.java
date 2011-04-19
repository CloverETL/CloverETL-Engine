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

import org.jetel.component.xml.writer.Mapping;
import org.jetel.component.xml.writer.MappingVisitor;
import org.jetel.util.string.StringUtils;

/**
 * @author LKREJCI (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 14 Dec 2010
 */
public class ObjectElement extends ObjectRepresentation {

	public static final boolean HIDE_DEFAULT = false;
	public static final boolean WRITE_NULL_DEFAULT = false;
	
	public static final MappingProperty[] AVAILABLE_PROPERTIES = {MappingProperty.NAME, 
		MappingProperty.WRITE_NULL_ELEMENT, MappingProperty.DATASCOPE, MappingProperty.KEY, MappingProperty.PARENTKEY,
		MappingProperty.HIDE, MappingProperty.FILTER, MappingProperty.PARTITION,
		MappingProperty.INCLUDE, MappingProperty.EXCLUDE, MappingProperty.WRITE_NULL_ATTRIBUTE, MappingProperty.OMIT_NULL_ATTRIBUTE};

	public static final MappingProperty[] AVAILABLE_PROPERTIES_SIMPLE = {MappingProperty.NAME, MappingProperty.VALUE,
		MappingProperty.WRITE_NULL_ELEMENT, MappingProperty.DATASCOPE, MappingProperty.KEY, MappingProperty.PARENTKEY,
		MappingProperty.HIDE, MappingProperty.FILTER, MappingProperty.PARTITION,
		MappingProperty.INCLUDE, MappingProperty.EXCLUDE, MappingProperty.WRITE_NULL_ATTRIBUTE, MappingProperty.OMIT_NULL_ATTRIBUTE};

	private List<ObjectRepresentation> children = new LinkedList<ObjectRepresentation>();
	private List<ObjectAttribute> attributes = new LinkedList<ObjectAttribute>();
	private List<ObjectNamespace> namespaces = new LinkedList<ObjectNamespace>();

	private RecurringElementInfo recurringInfo = null;
	private ObjectAggregate attributeInfo = null;
	
	private boolean template = false;

	public ObjectElement(ObjectElement parent) {
		super(parent, true);
	}

	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}
		
	public ObjectElement getRecurringParent() {
		if (parent != null) {
			if (parent.getRecurringInfo() != null) {
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
			throw new XMLStreamException(Mapping.UNKNOWN_ATTRIBUTE + localName);
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

		case DATASCOPE:
		case KEY:
		case PARENTKEY:
		case FILTER:
			if (recurringInfo == null) {
				recurringInfo = new RecurringElementInfo(this);
			}
			recurringInfo.setProperty(property, value);
			break;

		case EXCLUDE:
		case INCLUDE:
			if (attributeInfo == null) {
				attributeInfo = new ObjectAggregate(this, false);
			}
			attributeInfo.setProperty(property, value);
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
						ObjectValue objectValue = new ObjectValue(this);
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

		case DATASCOPE:
		case KEY:
		case PARENTKEY:
		case FILTER:
			if (recurringInfo == null) {
				return null;
			}
			return recurringInfo.getProperty(property);

		case EXCLUDE:
		case INCLUDE:
			if (attributeInfo == null) {
				return null;
			}
			return attributeInfo.getProperty(property);
			
		case VALUE:
			if (isSimple() && children.size() == 1) {
				return children.get(0).getProperty(MappingProperty.VALUE);
			} else {
				return null;
			}
			
		default:
			return null;
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
		ObjectAttribute attribute = new ObjectAttribute(this);
		attribute.setProperty(MappingProperty.NAME, name);
		attribute.setProperty(MappingProperty.VALUE, value);
		attributes.add(position, attribute);
	}
	
	public void addAttribute(int position, ObjectAttribute attribute) {
		attributes.add(position, attribute);
	}
	
	public void removeAttribute(int position) {
		attributes.remove(position);
	}

	public void addNamespace(int position, String prefix, String URI) {
		ObjectNamespace namespace = new ObjectNamespace(this);
		namespace.setProperty(MappingProperty.NAME, prefix);
		namespace.setProperty(MappingProperty.VALUE, URI);
		namespaces.add(position, namespace);
	}
	
	public void addNamespace(int position, ObjectNamespace namespace) {
		namespaces.add(position, namespace);
	}
	
	public void removeNamespace(int position) {
		namespaces.remove(position);
	}

	public void addChild(int position, ObjectRepresentation value) {
		children.add(position, value);
	}
	
	public void removeChild(int position) {
		children.remove(position);
	}

	public List<ObjectRepresentation> getChildren() {
		return Collections.unmodifiableList(children);
	}

	public void setChildren(List<ObjectRepresentation> children) {
		this.children = children;
	}

	public List<ObjectAttribute> getAttributes() {
		return Collections.unmodifiableList(attributes);
	}

	public List<ObjectNamespace> getNamespaces() {
		return Collections.unmodifiableList(namespaces);
	}

	public RecurringElementInfo getRecurringInfo() {
		return recurringInfo;
	}

	public void setRecurringInfo(RecurringElementInfo recurringInfo) {
		this.recurringInfo = recurringInfo;
	}

	public ObjectAggregate getAttributeInfo() {
		return attributeInfo;
	}

	public void setAttributeInfo(ObjectAggregate attributeInfo) {
		this.attributeInfo = attributeInfo;
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
		return (recurringInfo == null ? "static" : "recurring") + " ObjectElement [name=" 
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
		return recurringInfo == null && namespaces.isEmpty() && attributeInfo == null && attributes.isEmpty()
			&& (children.isEmpty() || (children.size() == 1 && children.get(0).getType() == ObjectRepresentation.VALUE));
	}
	
	public String getPath() {
		if (parent == null) {
			return ""; 
		} else {
			return super.getPath();
		}
	}

	@Override
	public short getType() {
		return template ? ObjectRepresentation.TEMPLATE : ObjectRepresentation.ELEMENT;
	}
}
