/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.data.tree.bean.schema.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.10.2011
 */
public class TypedObject extends SchemaObject {
	
	protected Map<String, SchemaObject> children = new LinkedHashMap<String, SchemaObject>();

	public TypedObject() {
		super(null);
	}
	
	public TypedObject(String type) {
		super(null);
		this.type = type;
	}
	
	@Override
	public void setParent(SchemaObject parent) {
		throw new UnsupportedOperationException("type cannot have parent");
	}

	@Override
	public void acceptVisitor(SchemaVisitor visitor) {
		for (SchemaObject child : children.values()) {
			child.acceptVisitor(visitor);
		}
		visitor.visit(this);
	}
	
	@Override
	public SchemaObject[] getChildren() {
		if (children.isEmpty()) {
			return null;
		}
		return children.values().toArray(new SchemaObject[children.size()]);
	}
	
	public int getChildCount() {
		return children.size();
	}
	
	public void addChild(SchemaObject child) {
		checkParent(child);
		if (!children.containsValue(child)) {
			children.put(child.getName(), child);
		}
	}
	
	public void clearChildren() {
		children.clear();
	}

	@Override
	public boolean hasChildren() {
		return children.size() > 0;
	}
	
	@Override
	public String getDefaultName() {
		return "typed-object";
	}
	
	public SchemaObject getChild(String name) {
		return children.get(name);
	}
	
	@Override
	public String getPath() {
		/*
		 * typed object itself is not
		 */
		return "";
	}
	
	/*
	 * Order of children in typed object is not significant.
	 */
	@Override
	public boolean isEqual(SchemaObject otherObject) {
		
		return super.isEqual(otherObject, false);
	}
	
	@Override
	public String toString() {
		return getDefaultName() + "#" + type;
	}
}
