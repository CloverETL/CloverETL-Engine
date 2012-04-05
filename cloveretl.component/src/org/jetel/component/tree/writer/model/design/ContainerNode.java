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
package org.jetel.component.tree.writer.model.design;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.jetel.util.string.StringUtils;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 5.1.2012
 */
public abstract class ContainerNode extends AbstractNode {

	private Relation relation = null;
	private List<AbstractNode> children = new LinkedList<AbstractNode>();
	private List<Namespace> namespaces = new LinkedList<Namespace>();
	
	private WildcardAttribute wildcardAttribute = null;
	private List<Attribute> attributes = new LinkedList<Attribute>();

	
	public ContainerNode(ContainerNode parent) {
		super(parent, true);
	}

	public void setAttribute(String localName, String value) throws XMLStreamException {
		MappingProperty property = MappingProperty.fromString(localName);
		if (!setProperty(property, value)) {
			throw new XMLStreamException(TreeWriterMapping.UNKNOWN_ATTRIBUTE + "'" + localName + "'");
		}
	}

	@Override
	public boolean setProperty(MappingProperty property, String value) {
		switch (property) {
		case INPUT_PORT:
		case KEY:
		case PARENT_KEY:
		case FILTER:
			if (relation == null) {
				relation = new Relation(this);
			}
			relation.setProperty(property, value);
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
		case EXCLUDE:
		case INCLUDE:
			if (wildcardAttribute == null) {
				wildcardAttribute = new WildcardAttribute(this);
			}
			wildcardAttribute.setProperty(property, value);
			break;

		default:
			return super.setProperty(property, value);
		}
		return true;
	}

	@Override
	public String getProperty(MappingProperty property) {
		switch (property) {
		case INPUT_PORT:
		case KEY:
		case PARENT_KEY:
		case FILTER:
			if (relation == null) {
				return null;
			}
			return relation.getProperty(property);

		case VALUE:
			if (isSimple() && children.size() == 1) {
				return children.get(0).getProperty(MappingProperty.VALUE);
			} else {
				return null;
			}
		case EXCLUDE:
		case INCLUDE:
			if (wildcardAttribute == null) {
				return null;
			}
			return wildcardAttribute.getProperty(property);

		default:
			return super.getProperty(property);
		}
	}

	public boolean isSimple() {
		if (relation != null) {
			return false;
		}
		if (wildcardAttribute != null || !attributes.isEmpty()) {
			return false;
		}
		if (!namespaces.isEmpty()) {
			return false;
		}
		// container is not simple if has any children with one exception - it is simple when it has one child of type value. 
		if (!children.isEmpty() && !(children.size() == 1 && children.get(0).getType() == AbstractNode.VALUE)) {
			return false;
		}
		
		return true;
	}

	public ContainerNode getRecurringParent() {
		if (parent != null) {
			if (parent.getRelation() != null) {
				return parent;
			} else {
				return parent.getRecurringParent();
			}
		}
		return null;
	}

	@Override
	public String getSimpleContent() {
		if (isSimple() && children.size() == 1) {
			return children.get(0).getSimpleContent();
		}

		return null;
	}

	@Override
	public String getHierarchicalName() {
		if (parent == null) {
			return "";
		} else if (Boolean.parseBoolean(getProperty(MappingProperty.HIDE))) {
			return parent.getHierarchicalName();
		} else {
			return super.getHierarchicalName();
		}
	}

	@Override
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
	
	public boolean isPartition() {
		return Boolean.parseBoolean(getProperty(MappingProperty.PARTITION));
	}

	public void addChild(int position, AbstractNode value) {
		children.add(position, value);
	}

	public void removeChild(int position) {
		children.remove(position);
	}

	public void removeChild(AbstractNode child) {
		children.remove(child);
	}

	public List<AbstractNode> getChildren() {
		return Collections.unmodifiableList(children);
	}

	public void setChildren(List<AbstractNode> children) {
		this.children = children;
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

	public List<Namespace> getNamespaces() {
		return Collections.unmodifiableList(namespaces);
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

	public List<Attribute> getAttributes() {
		return Collections.unmodifiableList(attributes);
	}

	public WildcardAttribute getWildcardAttribute() {
		return wildcardAttribute;
	}

	public void setWildcardAttribute(WildcardAttribute wildcardAttribute) {
		this.wildcardAttribute = wildcardAttribute;
	}

	public Relation getRelation() {
		return relation;
	}

	public void setRelation(Relation recurringInfo) {
		this.relation = recurringInfo;
	}

	@Override
	public String toString() {
		return (relation == null ? "static" : "recurring") + " " + super.toString();
	}
}
