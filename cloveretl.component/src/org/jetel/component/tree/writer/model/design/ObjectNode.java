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

import org.jetel.component.tree.writer.util.MappingVisitor;

/**
 * Class representing xml element. Can be treated as a template which can be referenced.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 14 Dec 2010
 */
public class ObjectNode extends ContainerNode {
	
	public static final String XML_TEMPLATE_DEFINITION = "template";

	public static final boolean HIDE_DEFAULT = false;
	public static final boolean WRITE_NULL_DEFAULT = false;
	
	public static final String XML_ELEMENT_WITH_NAME_ATTRIBUTE = "element";

	private static final MappingProperty[] AVAILABLE_PROPERTIES = {
		MappingProperty.NAME, MappingProperty.WRITE_NULL_ELEMENT,
		MappingProperty.INPUT_PORT, MappingProperty.KEY,
		MappingProperty.PARENT_KEY, MappingProperty.HIDE,
		MappingProperty.FILTER, MappingProperty.PARTITION,
		MappingProperty.INCLUDE, MappingProperty.EXCLUDE,
		MappingProperty.WRITE_NULL_ATTRIBUTE, MappingProperty.OMIT_NULL_ATTRIBUTE,
		MappingProperty.DATA_TYPE
	};
	private static final MappingProperty[] AVAILABLE_PROPERTIES_SIMPLE = {
		MappingProperty.NAME, MappingProperty.VALUE,
		MappingProperty.WRITE_NULL_ELEMENT, MappingProperty.INPUT_PORT,
		MappingProperty.KEY, MappingProperty.PARENT_KEY,
		MappingProperty.HIDE, MappingProperty.FILTER,
		MappingProperty.PARTITION, MappingProperty.INCLUDE,
		MappingProperty.EXCLUDE, MappingProperty.WRITE_NULL_ATTRIBUTE,
		MappingProperty.OMIT_NULL_ATTRIBUTE, MappingProperty.DATA_TYPE
	};
	
	private boolean template = false;

	public ObjectNode(ContainerNode parent) {
		super(parent);
	}

	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}

	@Override
	public MappingProperty[] getAvailableProperties() {
		if (isSimple()) {
			return AVAILABLE_PROPERTIES_SIMPLE;
		}
		return AVAILABLE_PROPERTIES;
	}

	@Override
	public String getDisplayName() {
		return properties.get(MappingProperty.NAME);
	}

	@Override
	public short getType() {
		return template ? AbstractNode.TEMPLATE : AbstractNode.ELEMENT;
	}

	@Override
	public String getDescription() {
		return template ? "Template declaration" : "An XML element. Example: <element0>";
	}

	public boolean isTemplate() {
		return template;
	}

	public void setTemplate(boolean template) {
		this.template = template;
	}

}
