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
package org.jetel.component.xpathparser.mappping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 'Context' mapping element model.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.12.2011
 */
public class MappingContext extends MappingElement {
	
	private Integer outputPort;
	private String parentKeys[];
	private String generatedKeys[];
	private String sequenceField;
	private String sequenceId;
	
	private List<MappingElement> children = new ArrayList<MappingElement>();

	@Override
	public void acceptVisitor(MappingVisitor visitor) {
		
		visitor.visitBegin(this);
		for (MappingElement child : children) {
			child.acceptVisitor(visitor);
		}
		visitor.visitEnd(this);
	}
	
	public void addChild(MappingElement child) {
		if (!children.contains(child)) {
			child.setParent(this);
			children.add(child);
		}
	}
	
	public Integer getOutputPort() {
		return outputPort;
	}

	public void setOutputPort(Integer outputPort) {
		this.outputPort = outputPort;
	}

	public String[] getParentKeys() {
		return parentKeys;
	}

	public void setParentKeys(String[] parentKeys) {
		this.parentKeys = parentKeys;
	}

	public String[] getGeneratedKeys() {
		return generatedKeys;
	}

	public void setGeneratedKeys(String[] generatedKeys) {
		this.generatedKeys = generatedKeys;
	}

	public String getSequenceField() {
		return sequenceField;
	}

	public void setSequenceField(String sequenceField) {
		this.sequenceField = sequenceField;
	}

	public String getSequenceId() {
		return sequenceId;
	}

	public void setSequenceId(String sequenceId) {
		this.sequenceId = sequenceId;
	}

	public List<MappingElement> getChildren() {
		return Collections.unmodifiableList(children);
	}
}
