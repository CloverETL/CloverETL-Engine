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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.jetel.util.string.StringUtils;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 24.10.2011
 */
public class SchemaValidator extends BaseSchemaObjectVisitor {
	
	private static final String UNSDEFINED_CLASS_ERROR = "Data structure not set";
	private static final String UNDEFINED_KEY_ERROR = "Key data structure not set";
	private static final String UNDEFINED_VALUE_ERROR = "Value data structure not set";
	
	private Map<SchemaObject, String> errors = new HashMap<SchemaObject, String>();

	public void validate(SchemaObject object) {
		errors.clear();
		object.acceptVisitor(this);
	}
	
	public boolean containsErrors() {
		return !errors.isEmpty();
	}
	
	public boolean containsError(SchemaObject object) {
		return errors.containsKey(object);
	}
	
	public String getError(SchemaObject object) {
		return errors.get(object);
	}
	
	public Map<SchemaObject, String> getErrors() {
		return Collections.unmodifiableMap(errors);
	}
	
	@Override
	public void visit(SchemaCollection collection) {
		if (collection.getItem() == null) {
			errors.put(collection, UNSDEFINED_CLASS_ERROR);
		}
	}

	@Override
	public void visit(SchemaMap map) {
		if (map.getKey() == null){
			errors.put(map, UNDEFINED_KEY_ERROR);
		}
		if (map.getValue() == null) {
			errors.put(map, UNDEFINED_VALUE_ERROR);
		}
	}

	@Override
	public void visit(TypedObjectRef object) {
		if (object.getTypedObject() == null || StringUtils.isEmpty(object.getTypedObject().getType())) {
			errors.put(object, UNSDEFINED_CLASS_ERROR);
		}
	}
}
