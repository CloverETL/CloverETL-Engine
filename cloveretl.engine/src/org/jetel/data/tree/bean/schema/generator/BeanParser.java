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
package org.jetel.data.tree.bean.schema.generator;

import java.beans.PropertyDescriptor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.beanutils.PropertyUtils;
import org.jetel.data.tree.bean.SimpleTypes;
import org.jetel.data.tree.bean.schema.model.BaseSchemaObjectVisitor;
import org.jetel.data.tree.bean.schema.model.SchemaCollection;
import org.jetel.data.tree.bean.schema.model.SchemaMap;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.SchemaVisitor;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.10.2011
 */
public class BeanParser {
	
	/**
	 * Performs analysis of <code>TypedObjectRef</code>-s to check whether structural model of the mentioned type
	 * is present - if not, each such type is loaded and its structure is added.
	 * @param schemaObject
	 * @param classLoader - class loader to find referenced types
	 * @return schemaObject enriched with types' structures
	 */
	public static <T extends SchemaObject> T addReferencedTypes(T schemaObject, final ClassLoader classLoader) {
		
		final Map<String, SchemaObject> parsedTypes = new HashMap<String, SchemaObject>();
		final BeanParser parser = new BeanParser();
		final ParsingContext context = parser.new ParsingContext();
		
		
		SchemaVisitor visitor = new BaseSchemaObjectVisitor() {
			
			@Override
			public void visit(TypedObjectRef typedObjectRef) {
				
				if (typedObjectRef.getTypedObject() == null || typedObjectRef.getTypedObject().getType() == null) {
					return;
				}
				SchemaObject typeStructure = parsedTypes.get(typedObjectRef.getTypedObject().getType());
				if (typeStructure == null) {
					/*
					 * find referenced class and explore its structure
					 */
					Class<?> objectType = null;
					try {
						objectType = Class.forName(typedObjectRef.getTypedObject().getType(), true, classLoader);
					} catch (Exception e) {
						throw new RuntimeException("Could not find type: " + typedObjectRef.getTypedObject().getType(), e);
					}
					typeStructure = parser.parseClass(objectType, null, context);
					parsedTypes.put(objectType.getName(), typeStructure);
				}
				/*
				 * replace
				 */
				if (typeStructure instanceof TypedObjectRef) {
					TypedObject typedObject = ((TypedObjectRef)typeStructure).getTypedObject();
					typedObjectRef.setTypedObject(typedObject);
				} else {
					/*
					 * parsed type is collection or map - replace typed reference
					 * with the type structure
					 */
					replace(typedObjectRef.getParent(), typedObjectRef, typeStructure);
				}
			}
		};
		schemaObject.acceptVisitor(visitor);
		return schemaObject;
	}
	
	protected static void replace(SchemaObject parent, SchemaObject obsolete, SchemaObject replacement) {
		
		if (parent instanceof SchemaCollection) {
			SchemaCollection collection = (SchemaCollection)parent;
			replacement.setParent(collection);
			if (collection.getItem() == obsolete) {
				collection.setItem(replacement);
			}
			return;
		}
		if (parent instanceof SchemaMap) {
			SchemaMap map = (SchemaMap)parent;
			replacement.setParent(map);
			if (map.getKey() == obsolete){
				map.setKey(replacement);
			}
			if (map.getValue() == obsolete) {
				map.setValue(replacement);
			}
			return;
		}
	}
	
	/**
	 * Answers structural model of the typpe specified.
	 * @param type
	 * @return <code>TypedObjectRef</code> instance wrapping <code>TypeObject</code> describing type's structure
	 */
	public static SchemaObject parse(Class<?> type) {
		
		BeanParser parser = new BeanParser();
		return parser.parseType(type, null, parser.new ParsingContext());
	}
	
	protected SchemaObject parseType(Type type, SchemaObject container, ParsingContext context) {
		
		if (type instanceof Class) {
			Class<?> klass = (Class<?>)type;
			return parseClass(klass, container, context);
		}
		if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType)type;
			if (parameterizedType.getRawType() instanceof Class) {
				Class<?> rawClass = (Class<?>)parameterizedType.getRawType();
				
				if (Collection.class.isAssignableFrom(rawClass)) {
					SchemaCollection collection = new SchemaCollection(container);
					collection.setType(rawClass.getName());
					SchemaObject nestedObject = parseType(parameterizedType.getActualTypeArguments()[0], collection, context);
					collection.setItem(nestedObject);
					return collection;
				}
				
				if (Map.class.isAssignableFrom(rawClass)) {
					SchemaMap map = new SchemaMap(container);
					map.setType(rawClass.getName());
					map.setKey(parseType(parameterizedType.getActualTypeArguments()[0], map, context));
					map.setValue(parseType(parameterizedType.getActualTypeArguments()[1], map, context));
					return map;
				}
			}
		}
		if (type instanceof GenericArrayType) {
			throw new UnsupportedOperationException("Generic array introspection is not supported");
//			GenericArrayType genericArrayType = (GenericArrayType)type;
//			Type componenType = genericArrayType.getGenericComponentType();
//			
//			SchemaCollection array = new SchemaCollection(container);
//			array.setType(what is the type?);
//			array.setItem(parseType(componenType, array, context));
//			return array;
		}
		if (type instanceof TypeVariable) {
			TypeVariable<?> typeVariable = (TypeVariable<?>)type;
			/*
			 * TODO this needs to properly investigated
			 */
			return parseType(typeVariable.getBounds()[0], container, context);
		}
		return null;
	}
	
	protected SchemaObject parseClass(Class<?> type, SchemaObject container, ParsingContext context) {
		
		if (SimpleTypes.isSimpleType(type)) {
			TypedObject typedObject = context.getTypedObject(type.getName());
			if (typedObject == null) {
				typedObject = new TypedObject(type.getName());
				context.addTypedObject(typedObject);
			}
			return new TypedObjectRef(container, typedObject);
		}
		if (type.isArray()) {
			SchemaCollection array = new SchemaCollection(container);
			array.setType(type.getName());
			array.setItem(parseClass(type.getComponentType(), array, context));
			return array;
		}
		if (type.getGenericSuperclass() != type.getSuperclass()) {
			SchemaObject schemaObject = parseType(type.getGenericSuperclass(), container, context);
			if (schemaObject != null) {
				schemaObject.setType(type.getName());
				return schemaObject;
			}
		}
		for (Type genericInterface : type.getGenericInterfaces()) {
			SchemaObject schemaObject = parseType(genericInterface, container, context);
			if (schemaObject != null) {
				return schemaObject;
			}
		}
		if (Collection.class.isAssignableFrom(type)) {
			SchemaCollection collection = new SchemaCollection(container);
			collection.setType(type.getName());
			collection.setItem(new TypedObjectRef(collection, context.OBJECT));
			return collection;
		}
		if (Map.class.isAssignableFrom(type)) {
			SchemaMap map = new SchemaMap(container);
			map.setType(type.getName());
			map.setKey(new TypedObjectRef(map, context.OBJECT));
			map.setValue(new TypedObjectRef(map, context.OBJECT));
			return map;
		}
		
		TypedObject typedObject = context.getTypedObject(type.getName());
		
		if (typedObject == null) {
			typedObject = new TypedObject(type.getName());
			context.addTypedObject(typedObject);
			for (PropertyDescriptor descriptor : PropertyUtils.getPropertyDescriptors(type)) {
				if (descriptor.getReadMethod() == null || descriptor.getWriteMethod() == null) {
					continue;
				}
				Type propertyType = descriptor.getReadMethod().getGenericReturnType();
				SchemaObject propertySchema = parseType(propertyType, typedObject, context);
				propertySchema.setName(descriptor.getName());
				typedObject.addChild(propertySchema);
			}
		}
		return new TypedObjectRef(container, typedObject);
	}
	
	protected class ParsingContext {
		
		private final TypedObject OBJECT = new TypedObject(Object.class.getName());
		private final Map<String, TypedObject> typedObjects = new HashMap<String, TypedObject>();
		{
			addTypedObject(OBJECT);
		}
		
		public TypedObject getTypedObject(String typeName) {
			return typedObjects.get(typeName);
		}
		
		public void addTypedObject(TypedObject typedObject) {
			if (!typedObjects.containsKey(typedObject.getName())) {
				typedObjects.put(typedObject.getType(), typedObject);
			}
		}
	}
}
