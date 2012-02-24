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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10.11.2011
 */
public class SchemaObjectReplicator  {

	@SuppressWarnings("unchecked")
	public <T extends SchemaObject> T copy(T schemaObject) {
		
		SchemaReplicatingVisitor visitor = new SchemaReplicatingVisitor();
		
		if (schemaObject instanceof TypedObject) {
			TypedObjectRef objectRef = new TypedObjectRef(null, (TypedObject)schemaObject);
			objectRef.acceptVisitor(visitor);
			objectRef = (TypedObjectRef) visitor.getStack().pop();
			return (T)objectRef.getTypedObject();
		} else {
			schemaObject.acceptVisitor(visitor);
			return (T)visitor.getStack().pop();
		}
	}
	
	private static class SchemaReplicatingVisitor extends BaseSchemaObjectVisitor {
		
		private Stack<SchemaObject> stack = new Stack<SchemaObject>();
		private Map<String, TypedObject> types = new HashMap<String, TypedObject>();
		private Set<String> replicatedTypes = new HashSet<String>(); 
		
		/**
		 * @return the stack
		 */
		public Stack<SchemaObject> getStack() {
			return stack;
		}
		
		@Override
		public void visit(SchemaCollection collection) {
			
			SchemaCollection copy = (SchemaCollection)collection.clone();
			if (collection.getItem() != null) {
				SchemaObject itemCopy = stack.pop();
				itemCopy.setParent(copy);
				copy.setItem(itemCopy);
			}
			stack.push(copy);
		}
		
		@Override
		public void visit(SchemaMap map) {
			
			SchemaMap copy = (SchemaMap)map.clone();
			if (map.getValue() != null) {
				SchemaObject valueCopy = stack.pop();
				valueCopy.setParent(copy);
				copy.setValue(valueCopy);
			}
			if (map.getKey() != null) {
				SchemaObject keyCopy = stack.pop();
				keyCopy.setParent(copy);
				copy.setKey(keyCopy);
			}
			stack.push(copy);
		}
		
		@Override
		public void visit(TypedObjectRef typedObjectRef) {
			
			TypedObjectRef copy = (TypedObjectRef)typedObjectRef.clone();
			TypedObject typedObject = copy.getTypedObject();
			if (typedObject != null) {
				if (!types.containsKey(typedObject.getType())) {
					/*
					 * prevent loops
					 */
					types.put(typedObject.getType(), (TypedObject)typedObject.clone());
					/*
					 * copy type
					 */
					typedObject.acceptVisitor(this);
				}
				copy.setTypedObject(types.get(typedObject.getType()));
			}
			stack.push(copy);
		}
		
		@Override
		public void visit(TypedObject object) {
			
			if (!replicatedTypes.contains(object.getType())) {
				TypedObject copy = types.get(object.getType());
				copy.children = new LinkedHashMap<String, SchemaObject>();
				if (object.hasChildren()) {
					List<SchemaObject> childCopies = new ArrayList<SchemaObject>(object.getChildCount());
					for (int i = 0; i < object.getChildCount(); ++i) {
						SchemaObject childCopy = stack.pop();
						childCopies.add(0, childCopy);
					}
					for (SchemaObject childCopy : childCopies) {
						childCopy.setParent(copy);
						copy.addChild(childCopy);
					}
				}
				replicatedTypes.add(object.getType());
			}
		}
	}
}
