/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.data.tree.bean.schema.model;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.10.2011
 */
public abstract class BaseSchemaObjectVisitor implements SchemaVisitor {
	
	@Override
	public void visit(SchemaCollection collection) {}
	
	@Override
	public void visit(SchemaMap map) {}

	@Override
	public void visit(TypedObject object) {}
	
	@Override
	public void visit(TypedObjectRef typedObjectRef) {}
}
