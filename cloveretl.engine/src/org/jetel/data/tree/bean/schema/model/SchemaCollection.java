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
