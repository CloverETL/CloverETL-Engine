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

package org.jetel.data.tree.bean.schema.model;

import org.jetel.data.tree.bean.BeanConstants;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.10.2011
 */
public class TypedObjectRef extends SchemaObject {

	private TypedObject typedObject;
	
	public TypedObjectRef(SchemaObject parent, TypedObject typedObject) {
		super(parent);
		if (typedObject != null) {
			type = typedObject.getType();
		}
		this.typedObject = typedObject;
	}
	
	public TypedObjectRef(SchemaObject parent, String type, TypedObject typedObject) {
		super(parent);
		this.type = type;
		this.typedObject = typedObject;
	}
	
	/**
	 * @return the typedObject
	 */
	public TypedObject getTypedObject() {
		return typedObject;
	}
	
	/**
	 * @param typedObject the typedObject to set
	 */
	public void setTypedObject(TypedObject typedObject) {
		this.typedObject = typedObject;
	}
	
	@Override
	public SchemaObject[] getChildren() {
		return null;
	}

	/**
	 * Always <code>false</code> to preserve tree-like structure - 
	 * <code>TypedObject</code> is a different tree of its own.
	 */
	@Override
	public boolean hasChildren() {
		return false;
	}

	/**
	 * In order to prevent loops, referenced <code>TypedObject</code>
	 * is not visited - this needs to be done explicitly in visitor code
	 * if needed.
	 */
	@Override
	public void acceptVisitor(SchemaVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public String getDefaultName() {
		return BeanConstants.OBJECT_ELEMENT_NAME;
	}
}
