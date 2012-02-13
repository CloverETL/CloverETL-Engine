/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.data.tree.bean.util;

import org.jetel.data.tree.bean.schema.model.BaseSchemaObjectVisitor;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;

/**
 * A visitor to remove type structures from object schema.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.10.2011
 */
public class TypedObjectClearingVisitor extends BaseSchemaObjectVisitor {

	@Override
	public void visit(TypedObjectRef typedObjectRef) {
		if (typedObjectRef.getTypedObject() != null) {
			typedObjectRef.getTypedObject().clearChildren();
		}
	}
}
