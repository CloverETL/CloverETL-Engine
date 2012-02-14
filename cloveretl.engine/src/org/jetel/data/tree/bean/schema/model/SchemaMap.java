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
 * TODO discussion needed: in order to simplify some algorithms, it could be handy to insert some "SchemaMapEntry"
 * structure as contained element of the <code>SchemaMap</code>
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 20.10.2011
 */
public class SchemaMap extends SchemaObject {

	private static final String RELATIONAL_PATH_KEY = BeanConstants.PATH_SEPARATOR + BeanConstants.MAP_ENTRY_ELEMENT_NAME + BeanConstants.PATH_SEPARATOR + BeanConstants.MAP_KEY_ELEMENT_NAME;
	private static final String RELATIONAL_PATH_VALUE = BeanConstants.PATH_SEPARATOR + BeanConstants.MAP_ENTRY_ELEMENT_NAME + BeanConstants.PATH_SEPARATOR + BeanConstants.MAP_VALUE_ELEMENT_NAME;

	private SchemaObject key;
	private SchemaObject value;

	public SchemaMap(SchemaObject parent) {
		super(parent);
	}

	/**
	 * Order of visiting: key, value, this.
	 */
	@Override
	public void acceptVisitor(SchemaVisitor visitor) {
		if (key != null) {
			key.acceptVisitor(visitor);
		}
		if (value != null) {
			value.acceptVisitor(visitor);
		}
		visitor.visit(this);
	}

	@Override
	public SchemaObject[] getChildren() {
		return new SchemaObject[] { key, value };
	}

	@Override
	public boolean hasChildren() {
		return true;
	}

	public SchemaObject getKey() {
		return key;
	}

	public void setKey(SchemaObject key) {
		checkParent(key);
		this.key = key;
	}

	public SchemaObject getValue() {
		return value;
	}

	public void setValue(SchemaObject value) {
		checkParent(value);
		this.value = value;
	}

	@Override
	public String getRelationPath(SchemaObject schemaObject) {

		if (key == schemaObject) {
			return RELATIONAL_PATH_KEY;
		}
		if (value == schemaObject) {
			return RELATIONAL_PATH_VALUE;
		}
		return super.getRelationPath(schemaObject);
	}

	@Override
	public String getDefaultName() {
		return BeanConstants.MAP_ELEMENT_NAME;
	}
}
