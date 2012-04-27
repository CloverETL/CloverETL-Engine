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
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 9.1.2012
 */
public class CollectionNode extends ContainerNode {
	
	public static final String XML_COLLECTION_DEFINITION = "collection";

	private static final MappingProperty[] AVAILABLE_PROPERTIES = { MappingProperty.NAME, MappingProperty.WRITE_NULL_ELEMENT, MappingProperty.INPUT_PORT, MappingProperty.KEY, MappingProperty.PARENT_KEY, MappingProperty.FILTER, MappingProperty.PARTITION};

	public CollectionNode(ContainerNode parent) {
		super(parent);
	}

	@Override
	public short getType() {
		return AbstractNode.COLLECTION;
	}

	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}

	@Override
	public String getDisplayName() {
		return properties.get(MappingProperty.NAME);
	}

	@Override
	public String getDescription() {
		return "An array node. Example: [value1, value2]";
	}

	@Override
	MappingProperty[] getAvailableProperties() {
		return AVAILABLE_PROPERTIES;
	}

}
