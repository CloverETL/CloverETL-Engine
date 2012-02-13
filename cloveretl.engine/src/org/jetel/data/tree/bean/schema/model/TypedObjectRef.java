/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
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
