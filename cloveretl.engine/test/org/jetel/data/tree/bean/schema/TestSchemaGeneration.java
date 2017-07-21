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
package org.jetel.data.tree.bean.schema;

import java.util.HashMap;

import org.apache.ws.commons.schema.XmlSchema;
import org.jetel.data.tree.bean.ComplexTestType;
import org.jetel.data.tree.bean.ComplexTestTypeWithArray;
import org.jetel.data.tree.bean.ComplexTestTypeWithCollection;
import org.jetel.data.tree.bean.IntegerArray;
import org.jetel.data.tree.bean.ListHolder;
import org.jetel.data.tree.bean.ListOfMapsOfMaps;
import org.jetel.data.tree.bean.MapIntString;
import org.jetel.data.tree.bean.SimpleTestType;
import org.jetel.data.tree.bean.schema.generator.BeanParser;
import org.jetel.data.tree.bean.schema.generator.SchemaGenerator;
import org.jetel.data.tree.bean.schema.model.SchemaCollection;
import org.jetel.data.tree.bean.schema.model.SchemaMap;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.10.2011
 */
public class TestSchemaGeneration {

	@Test
	public void testSimpleObject() throws Exception {
		printTestMethod();
		
		SchemaObject typeStructure = BeanParser.parse(SimpleTestType.class);
		
		Assert.assertEquals(TypedObjectRef.class, typeStructure.getClass());
		
		TypedObjectRef typedObjectRef = (TypedObjectRef)typeStructure;
		Assert.assertEquals(2, typedObjectRef.getTypedObject().getChildren().length);
		
		XmlSchema objectXmlSchema = SchemaGenerator.generateSchema(typedObjectRef.getTypedObject());
		objectXmlSchema.write(System.out);
	}
	
	@Test
	public void testComplexObject() throws Exception {
		printTestMethod();
		
		SchemaObject typeStructure = BeanParser.parse(ComplexTestType.class);
		
		Assert.assertEquals(TypedObjectRef.class, typeStructure.getClass());
		
		TypedObjectRef typedObjectRef = (TypedObjectRef)typeStructure;
		Assert.assertEquals(2, typedObjectRef.getTypedObject().getChildren().length);
		
		XmlSchema objectXmlSchema = SchemaGenerator.generateSchema(typedObjectRef);
		objectXmlSchema.write(System.out);
	}
	
	@Test
	public void testComplexObjectWithCollection() throws Exception {
		printTestMethod();
		
		SchemaObject typeStructure = BeanParser.parse(ComplexTestTypeWithCollection.class);
		
		Assert.assertEquals(TypedObjectRef.class, typeStructure.getClass());
		
		TypedObjectRef typedObjectRef = (TypedObjectRef)typeStructure;
		Assert.assertEquals(3, typedObjectRef.getTypedObject().getChildren().length);
		
		XmlSchema objectXmlSchema = SchemaGenerator.generateSchema(typedObjectRef);
		objectXmlSchema.write(System.out);
	}
	
	@Test
	public void testComplexObjectWithArray() throws Exception {
		printTestMethod();
		
		SchemaObject typeStructure = BeanParser.parse(ComplexTestTypeWithArray.class);
		
		Assert.assertEquals(TypedObjectRef.class, typeStructure.getClass());
		
		TypedObjectRef typedObjectRef = (TypedObjectRef)typeStructure;
		Assert.assertEquals(4, typedObjectRef.getTypedObject().getChildren().length);
		
		XmlSchema objectXmlSchema = SchemaGenerator.generateSchema(typedObjectRef);
		objectXmlSchema.write(System.out);
	}
	
	@Test
	public void testMap() throws Exception {
		printTestMethod();
		/*
		 * Map<String, SimpleTestType>
		 */
		SchemaMap map = new SchemaMap(null);
		TypedObject typedObject = new TypedObject(String.class.getName());
		TypedObjectRef ref = new TypedObjectRef(map, typedObject);
		map.setKey(ref);
		typedObject = new TypedObject(SimpleTestType.class.getName());
		ref = new TypedObjectRef(map, typedObject);
		map.setValue(ref);
		
		map = BeanParser.addReferencedTypes(map, getClass().getClassLoader());
		
		XmlSchema xmlSchema = SchemaGenerator.generateSchema(map);
		xmlSchema.write(System.out);
		
		
	}
	
	@Test
	public void testMapImplementationInStructure() throws Exception {
		printTestMethod();
		
		SchemaCollection collection = new SchemaCollection(null);
		SchemaMap map = new SchemaMap(collection);
		collection.setItem(map);
		map.setKey(new TypedObjectRef(map, new TypedObject(String.class.getName())));
		map.setValue(new TypedObjectRef(map, new TypedObject(HashMap.class.getName())));
		
		collection = BeanParser.addReferencedTypes(collection, getClass().getClassLoader());
		
		XmlSchema xmlSchema = SchemaGenerator.generateSchema(collection);
		xmlSchema.write(System.out);
	}
	
	@Test
	public void testParametrizedMapImplementationInStructure() throws Exception {
		printTestMethod();
		
		SchemaCollection collection = new SchemaCollection(null);
		collection.setItem(new TypedObjectRef(collection, new TypedObject(MapIntString.class.getName())));
		
		collection = BeanParser.addReferencedTypes(collection, getClass().getClassLoader());
		
		XmlSchema xmlSchema = SchemaGenerator.generateSchema(collection);
		xmlSchema.write(System.out);
	}

	@Test
	public void testListOfMapsOfMaps() throws Exception {
		printTestMethod();
		
		SchemaObject typeStructure = BeanParser.parse(ListOfMapsOfMaps.class);
		
		Assert.assertEquals(ListOfMapsOfMaps.class.getName(), typeStructure.getType());
		
		XmlSchema xmlSchema = SchemaGenerator.generateSchema(typeStructure);
		xmlSchema.write(System.out);
	}
	
	@Test
	public void testListHolder() throws Exception {
		printTestMethod();
		
		SchemaObject typeStructure = BeanParser.parse(ListHolder.class);
		
		XmlSchema xmlSchema = SchemaGenerator.generateSchema(typeStructure);
		xmlSchema.write(System.out);
	}
	
	@Test
	public void testGenericArray() throws Exception {
		printTestMethod();
		
		SchemaObject typeStructure = BeanParser.parse(IntegerArray.class);
		
		XmlSchema xmlSchema = SchemaGenerator.generateSchema(typeStructure);
		xmlSchema.write(System.out);
	}
	
	private void printTestMethod() {
		
		System.out.println("\n" + Thread.currentThread().getStackTrace()[2].getMethodName());
	}
}
