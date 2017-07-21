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

import java.util.HashMap;
import java.util.Map;

/**
 * Utility to compare two SchemaObject structures including structure of the referenced types.
 * 
 * @see SchemaObject#isEqual(SchemaObject)
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7.11.2011
 */
public class SchemaObjectComparator  {

	public Map<SchemaObject, SchemaObject> compare(SchemaObject object1, SchemaObject object2) {
		
		Map<SchemaObject, SchemaObject> differencies = new HashMap<SchemaObject, SchemaObject>();
		/*
		 * compare top level structures
		 */
		if (!object1.isEqual(object2)) {
			differencies.put(object1, object2);
			return differencies;
		}
		/*
		 * collect types
		 */
		TypeCollector collector1 = new TypeCollector();
		TypeCollector collector2 = new TypeCollector();
		object1.acceptVisitor(collector1);
		object2.acceptVisitor(collector2);
		for (Map.Entry<String, TypedObject> entry : collector1.getTypes().entrySet()) {
			TypedObject type1 = entry.getValue();
			TypedObject type2 = collector2.getTypes().get(entry.getKey());
			if (type1 != null && type2 != null) {
				/*
				 * compare type structures
				 */
				if (!type1.isEqual(type2)) {
					differencies.put(type1, type2);
				}
			}
		}
		return differencies;
	}
	
	protected static class TypeCollector extends BaseSchemaObjectVisitor {
		
		private Map<String, TypedObject> types = new HashMap<String, TypedObject>();
		
		@Override
		public void visit(TypedObjectRef typedObjectRef) {
			
			TypedObject typedObject = typedObjectRef.getTypedObject();
			if (typedObject != null) {
				boolean known = types.containsKey(typedObject.getType());
				types.put(typedObject.getType(), typedObject);
				if (!known) {
					typedObject.acceptVisitor(this);
				}
			}
		}
		
		/**
		 * @return the types
		 */
		public Map<String, TypedObject> getTypes() {
			return types;
		}
	}
}
