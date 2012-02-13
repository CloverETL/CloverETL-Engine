/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
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
