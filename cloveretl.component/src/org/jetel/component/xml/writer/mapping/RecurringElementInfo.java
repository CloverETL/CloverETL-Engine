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

import org.jetel.component.xml.writer.MappingVisitor;

import com.sleepycat.je.tree.Key;

/**
 * @author LKREJCI (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 15 Dec 2010
 */
public class RecurringElementInfo extends ObjectRepresentation {
	
	public static final boolean PARTITION_DEFAULT = false;

	public static final MappingProperty[] AVAILABLE_PROPERTIES = {MappingProperty.DATASCOPE,
		MappingProperty.KEY, MappingProperty.PARENTKEY, MappingProperty.FILTER};

	public RecurringElementInfo(ObjectElement parent) {
		super(parent, false);
	}
	
	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}

	@Override
	public String getSimpleContent() {
		StringBuilder sb = new StringBuilder();
		sb.append("Port: '").append(getProperty(MappingProperty.DATASCOPE)).append("'");
		
		String property = getProperty(MappingProperty.KEY);
		if (property != null) {
			sb.append(" Key: ").append(property).append("'");
		}
		
		property = getProperty(MappingProperty.PARENTKEY);
		if (property != null) {
			sb.append(" Parent Key: ").append(property).append("'");
		}
		return sb.toString();
	}

	@Override
	public String getDisplayName() {
		return "Relation";
	}

	@Override
	public MappingProperty[] getAvailableProperties() {
		return AVAILABLE_PROPERTIES;
	}

	@Override
	public short getType() {
		return ObjectRepresentation.RELATION;
	}
}
