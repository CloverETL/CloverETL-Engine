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
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 20.10.2011
 */
public class SchemaCollection extends SchemaObject {

	private SchemaObject item;

	public SchemaCollection(SchemaObject parent) {
		super(parent);
	}

	@Override
	public SchemaObject[] getChildren() {
		return new SchemaObject[] { item };
	}

	@Override
	public void acceptVisitor(SchemaVisitor visitor) {
		if (item != null) {
			item.acceptVisitor(visitor);
		}
		visitor.visit(this);
	}

	@Override
	public boolean hasChildren() {
		return true;
	}

	public SchemaObject getItem() {
		return item;
	}

	public void setItem(SchemaObject item) {
		checkParent(item);
		this.item = item;
	}

	@Override
	public String getRelationPath(SchemaObject schemaObject) {
		if (schemaObject == item) {
			return "";
		}
		return super.getRelationPath(schemaObject);
	}

	@Override
	public String getPath() {
		String path = "";
		if (parent != null) {
			/*
			 * my path is parent path + relation to the parent
			 */
			path += parent.getPath();
			path += parent.getRelationPath(this);
		}
		return path;
	}

	@Override
	public String getDefaultName() {
		return parent != null ? BeanConstants.LIST_ITEM_ELEMENT_NAME : BeanConstants.LIST_ELEMENT_NAME;
	}
}
