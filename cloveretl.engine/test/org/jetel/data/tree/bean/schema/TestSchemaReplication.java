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

import java.util.Map;

import org.apache.ws.commons.schema.XmlSchema;
import org.jetel.data.tree.bean.ComplexTestType;
import org.jetel.data.tree.bean.SimpleTestType;
import org.jetel.data.tree.bean.schema.generator.BeanParser;
import org.jetel.data.tree.bean.schema.generator.SchemaGenerator;
import org.jetel.data.tree.bean.schema.model.SchemaMap;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.SchemaObjectComparator;
import org.jetel.data.tree.bean.schema.model.SchemaObjectReplicator;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;
import org.jetel.data.tree.bean.util.TypedObjectClearingVisitor;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10.11.2011
 */
public class TestSchemaReplication {
	
	@Test
	public void testSimpleType() {
		
		SchemaObject simpleSchema = BeanParser.parse(SimpleTestType.class);
		
		XmlSchema schema = SchemaGenerator.generateSchema(simpleSchema);
		schema.write(System.out);
		
		SchemaObject copy = new SchemaObjectReplicator().copy(simpleSchema);
		
		schema = SchemaGenerator.generateSchema(copy);
		schema.write(System.out);
		
		Assert.assertTrue(copy.isEqual(simpleSchema));
		
		Map<SchemaObject, SchemaObject> diff = new SchemaObjectComparator().compare(simpleSchema, copy);
		
		Assert.assertEquals(0, diff.size());
	}
	
	@Test
	public void testCompexClone() {
		
		SchemaObject complexSchema = BeanParser.parse(ComplexTestType.class);
		
		XmlSchema schema = SchemaGenerator.generateSchema(complexSchema);
		schema.write(System.out);
		
		SchemaObject complexSchemaCopy = new SchemaObjectReplicator().copy(complexSchema);
		
		schema = SchemaGenerator.generateSchema(complexSchemaCopy);
		schema.write(System.out);
		
		Assert.assertTrue(complexSchema.isEqual(complexSchemaCopy));
		
		Map<SchemaObject, SchemaObject> diff = new SchemaObjectComparator().compare(complexSchema, complexSchemaCopy);
		
		Assert.assertTrue(diff.isEmpty());
	}
	
	@Test
	public void testMapWithReferences() throws Exception {
		
		SchemaMap map = new SchemaMap(null);
		
		TypedObjectRef stringRef = new TypedObjectRef(map, new TypedObject(String.class.getName()));
		TypedObjectRef complexTypeRef = new TypedObjectRef(map, new TypedObject(ComplexTestType.class.getName()));
		
		map.setKey(stringRef);
		map.setValue(complexTypeRef);
		
		map = BeanParser.addReferencedTypes(map, getClass().getClassLoader());
		
		SchemaMap mapCopy = new SchemaObjectReplicator().copy(map);
		mapCopy.acceptVisitor(new TypedObjectClearingVisitor());
		mapCopy = BeanParser.addReferencedTypes(mapCopy, getClass().getClassLoader());
		
		Map<SchemaObject, SchemaObject> diff = new SchemaObjectComparator().compare(map, mapCopy);
		
		Assert.assertTrue(diff.isEmpty());
	}
}
