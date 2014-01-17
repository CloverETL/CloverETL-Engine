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

import java.util.HashMap;
import java.util.Map;

import org.jetel.component.tree.writer.util.MappingVisitor;
import org.jetel.util.string.StringUtils;

/**
 * Abstract class for model elements
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 14 Dec 2010
 */
public abstract class AbstractNode {
	
	public static final short AGGREGATE_ELEMENT = 0;
	public static final short AGGREGATE_ATTRIBUTE = 1;
	public static final short ATTRIBUTE = 2;
	public static final short ELEMENT = 3;
	public static final short TEMPLATE = 4;
	public static final short NAMESPACE = 5;
	public static final short TEMPLATE_ENTRY = 6;
	public static final short VALUE = 7;
	public static final short RELATION = 8;
	public static final short COMMENT = 9;
	public static final short COLLECTION = 10;
	public static final short CDATA = 11;
	
	public static final String LEVEL_DELIMITER = "/";

	protected ContainerNode parent;
	protected Map<MappingProperty, String> properties = new HashMap<MappingProperty, String>();

	
	public AbstractNode(ContainerNode parent, boolean registerAsChild) {
		this.parent = parent;
		if (registerAsChild && parent != null) {
			parent.addChild(parent.getChildren().size(), this);
		}
	}
	
	public ContainerNode getParent() {
		return parent;
	}

	public void setParent(ContainerNode parent) {
		this.parent = parent;
	}
	
	public String getProperty(MappingProperty property) {
		if (property == MappingProperty.PATH) {
			return getPath();
		} else if (property == MappingProperty.DESCRIPTION) {
			return getDescription();
		}
		for (MappingProperty availableProperty : getAvailableProperties()) {
			if (property == availableProperty) {
				return properties.get(property);
			}
		}
		return null;
	}
	
	public boolean setProperty(MappingProperty property, String value) {
		for (MappingProperty availableProperty : getAvailableProperties()) {
			if (property == availableProperty) {
				if (StringUtils.isEmpty(value)) {
					properties.remove(property);
				} else {
					properties.put(property, value);
				}
				return true;
			}
		}
		return false;
	}
	
	public String getPath() {
		return parent.getPath() + AbstractNode.LEVEL_DELIMITER + getDisplayName();
	}
	
	public String getHierarchicalName() {
		return parent.getHierarchicalName() + AbstractNode.LEVEL_DELIMITER + getDisplayName();
	}
	
	public abstract short getType();
	
	public abstract void accept(MappingVisitor visitor) throws Exception;
	
	public abstract String getSimpleContent();
	public abstract String getDisplayName();
	public abstract String getDescription();
	
	abstract MappingProperty[] getAvailableProperties();

	@Override
	public String toString() {
		return getDisplayName() + "\n Props: " + properties;
	}
	
}
