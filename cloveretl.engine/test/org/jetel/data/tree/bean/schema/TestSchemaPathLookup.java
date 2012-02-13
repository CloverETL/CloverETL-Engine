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

import org.jetel.data.tree.bean.ComplexTestType;
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
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30.10.2011
 */
public class TestSchemaPathLookup {

	@Test
	public void testSimpleSchemaLookup() {
		
		SchemaCollection collection = new SchemaCollection(null);
		SchemaMap map = new SchemaMap(collection);
		collection.setItem(map);
		TypedObject object = new TypedObject("java.lang.String");
		map.setKey(new TypedObjectRef(map, object));
		map.setValue(new TypedObjectRef(map, object));
		
		SchemaGenerator.generateSchema(collection).write(System.out);
		
		Assert.assertNull(SchemaGenerator.findSchemaObject("/", collection));
		Assert.assertEquals(collection, SchemaGenerator.findSchemaObject("/list", collection));
		Assert.assertEquals(map, SchemaGenerator.findSchemaObject("/list/item", collection));
		Assert.assertEquals(map, SchemaGenerator.findSchemaObject("/list/item/entry", collection));
		Assert.assertEquals(map.getKey(), SchemaGenerator.findSchemaObject("/list/item/entry/key", collection));
	}
	
	@Test
	public void testComplexSchema() {
		
		SchemaObject schema = BeanParser.parse(ComplexTestType.class);
		
		SchemaGenerator.generateSchema(schema).write(System.out);
		
		Assert.assertEquals(schema, SchemaGenerator.findSchemaObject("/object", schema));
		Assert.assertEquals("otherComplexType", SchemaGenerator.findSchemaObject("/object/otherComplexType", schema).getName());
		Assert.assertEquals("mapStringToSimpleType", SchemaGenerator.findSchemaObject("/object/mapStringToSimpleType", schema).getName());
		Assert.assertEquals(TypedObjectRef.class, SchemaGenerator.findSchemaObject("/object/mapStringToSimpleType/entry/key", schema).getClass());
		/*
		 * test circulation
		 */
		Assert.assertEquals(TypedObjectRef.class, SchemaGenerator.findSchemaObject("/object/otherComplexType/otherComplexType/mapStringToSimpleType/entry/key", schema).getClass());
		Assert.assertEquals(TypedObjectRef.class, SchemaGenerator.findSchemaObject("/object/otherComplexType/mapStringToSimpleType/entry/value/intValue", schema).getClass());
	}
}
