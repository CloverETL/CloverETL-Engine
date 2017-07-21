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


/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14 Dec 2010
 */
public enum MappingProperty {

	EXCLUDE("exclude"),
	FILTER("filter"),
	HIDE("hide"), 		//True|False value
	INCLUDE("include"),	//True|False value if property it's property of ObjectComment
	INPUT_PORT("inPort"),
	KEY("key"),
	NAME("name"),
	PARENT_KEY("parentKey"),
	PARTITION("partition"), 	//True|False value
	VALUE("value"),
	WRITE("write"),
	INDEX("index"), // position of attribute among all attributes
	
	OMIT_NULL_ATTRIBUTE("omitNullAttribute"),
	WRITE_NULL_ATTRIBUTE("writeNullAttribute"),
	OMIT_NULL_ELEMENT("omitNullElement"),
	WRITE_NULL_ELEMENT("writeNullElement"), //True|False it's property of ObjectElement
	
	DATA_TYPE("dataType"), // value data type (useful for Maps)
	
	PATH("path"), //read-only property
	DESCRIPTION("description"), //read-only property
	UNKNOWN("unknown");

	private static final Map<String, MappingProperty> lookup = new HashMap<String, MappingProperty>();
	static {
		for (MappingProperty property : MappingProperty.values()) {
			lookup.put(property.getName().toLowerCase(), property);
		}
	}
	
	private final String name;
	
	private MappingProperty(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
	
	public static MappingProperty fromString(String keyword) {
		MappingProperty property = lookup.get(keyword.toLowerCase());
		return property != null ? property : UNKNOWN;
	}
}
