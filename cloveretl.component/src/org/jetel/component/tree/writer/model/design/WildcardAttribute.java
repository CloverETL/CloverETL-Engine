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
 * @created 5.1.2012
 */
public class WildcardAttribute extends AbstractNode {

	private static final MappingProperty[] AVAILABLE_PROPERTIES_ATTRIBUTE = { MappingProperty.INCLUDE, MappingProperty.EXCLUDE };

	public WildcardAttribute(ContainerNode parent) {
		super(parent, false);
	}

	@Override
	public short getType() {
		return AbstractNode.AGGREGATE_ATTRIBUTE;
	}

	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}

	@Override
	public String getSimpleContent() {
		StringBuilder sb = new StringBuilder();
		String property = getProperty(MappingProperty.INCLUDE);
		if (property != null) {
			sb.append("Include: '").append(property).append("'");
		}
		
		property = getProperty(MappingProperty.EXCLUDE);
		if (property != null) {
			sb.append(" Exclude: '").append(property).append("'");
		}
		
		return sb.toString();
	}

	@Override
	public String getDisplayName() {
		return "Wildcard attribute";
	}

	@Override
	public String getDescription() {
		return WildcardNode.DESCRIPTION + "attributes";
	}

	@Override
	MappingProperty[] getAvailableProperties() {
		return AVAILABLE_PROPERTIES_ATTRIBUTE;
	}

}
