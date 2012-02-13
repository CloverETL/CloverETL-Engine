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
