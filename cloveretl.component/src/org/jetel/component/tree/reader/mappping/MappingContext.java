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
package org.jetel.component.tree.reader.mappping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
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
	
	private List<FieldMapping> fieldMappingChildren = new LinkedList<FieldMapping>();
	private List<MappingContext> mappingContextChildren = new LinkedList<MappingContext>();

	@Override
	public void acceptVisitor(MappingVisitor visitor) {
		
		visitor.visitBegin(this);
		for (MappingElement child : getChildren()) {
			child.acceptVisitor(visitor);
		}
		visitor.visitEnd(this);
	}
	
	@Override
	public MappingContext getParent() {
		return (MappingContext)super.getParent();
	}
	
	public void addChild(MappingContext contextChild) {
		if (!mappingContextChildren.contains(contextChild)) {
			contextChild.setParent(this);
			mappingContextChildren.add(contextChild);
		}
	}
	
	public void addChild(FieldMapping fieldChild) {
		if (!fieldMappingChildren.contains(fieldChild)) {
			fieldChild.setParent(this);
			fieldMappingChildren.add(fieldChild);
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
		List<MappingElement> result = new ArrayList<MappingElement>(mappingContextChildren.size() + fieldMappingChildren.size());
		result.addAll(fieldMappingChildren);
		result.addAll(mappingContextChildren);
		return result;
	}
	
	public List<FieldMapping> getFieldMappingChildren() {
		return Collections.unmodifiableList(fieldMappingChildren);
	}
	
	public List<MappingContext> getMappingContextChildren() {
		return Collections.unmodifiableList(mappingContextChildren);
	}
}
