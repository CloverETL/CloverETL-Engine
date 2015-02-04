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
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19.11.2013
 */
public class CDataSection extends AbstractNode {

	private static final MappingProperty[] AVAILABLE_PROPERTIES = {MappingProperty.VALUE};

	public CDataSection(ContainerNode parent) {
		super(parent, true);
	}
	
	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}

	@Override
	public String getSimpleContent() {
		return properties.get(MappingProperty.VALUE);
	}

	@Override
	public String getDisplayName() {
		return "CDATA";
	}

	@Override
	public MappingProperty[] getAvailableProperties() {
		return AVAILABLE_PROPERTIES;
	}

	@Override
	public short getType() {
		return AbstractNode.CDATA;
	}

	@Override
	public String getDescription() {
		return "A CDATA section. Example: <![CDATA[ <xml /> ]]>";
	}
}
