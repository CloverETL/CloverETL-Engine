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

import java.util.HashMap;
import java.util.Map;

import org.jetel.component.xml.writer.MappingVisitor;
import org.jetel.util.string.StringUtils;

/**
 * @author LKREJCI (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 14 Dec 2010
 */
public abstract class ObjectRepresentation {
	
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
	
	public static final String LEVEL_DELIMITER = "/";

	protected ObjectElement parent;
	protected Map<MappingProperty, String> properties = new HashMap<MappingProperty, String>();

	protected int startOffset;
	protected int length;

	public ObjectRepresentation(ObjectElement parent, boolean registerAsChild) {
		this.parent = parent;
		if (parent != null && registerAsChild) {
			parent.addChild(parent.getChildren().size(), this);
		}
	}

	public abstract void accept(MappingVisitor visitor) throws Exception;	

	public long getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(int startOffset) {
		this.startOffset = startOffset;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public void setParent(ObjectElement parent) {
		this.parent = parent;
	}

	public ObjectElement getParent() {
		return parent;
	}
	
	public String getPath() {
		return parent.getPath() + ObjectRepresentation.LEVEL_DELIMITER + getDisplayName();
	}
	
	public String getProperty(MappingProperty property) {
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
	
	public abstract String getSimpleContent();
	public abstract String getDisplayName();
	
	public abstract MappingProperty[] getAvailableProperties();
	public abstract short getType();
	
}
