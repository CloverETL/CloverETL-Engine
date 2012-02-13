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

import org.jetel.data.tree.bean.schema.model.SchemaMap;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.SchemaObjectComparator;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7.11.2011
 */
public class TestSchemaComparison {

	@Test
	public void testSimpleStructureComparison() {
		
		TypedObject string = new TypedObject("java.util.String");
		
		SchemaMap map = new SchemaMap(null);
		map.setKey(new TypedObjectRef(map, string));
		
		SchemaMap innerMap = new SchemaMap(map);
		map.setValue(innerMap);
		innerMap.setKey(new TypedObjectRef(innerMap, "java.lang.String", string));
		innerMap.setValue(new TypedObjectRef(innerMap, "java.lang.Object", new TypedObject("java.lang.Object")));
		
		
		TypedObject string2 = new TypedObject("java.util.String");
		
		SchemaMap map2 = new SchemaMap(null);
		map2.setKey(new TypedObjectRef(map2, string2));
		
		SchemaMap innerMap2 = new SchemaMap(map2);
		map2.setValue(innerMap2);
		innerMap2.setKey(new TypedObjectRef(innerMap2, "java.lang.String", string2));
		innerMap2.setValue(new TypedObjectRef(innerMap2, "java.lang.Object", new TypedObject("java.lang.Object")));
		
		Assert.assertTrue(map.isEqual(map2));
		
		innerMap2.setValue(new TypedObjectRef(innerMap2, "java.lang.String", string2));
		
		Assert.assertFalse(map.isEqual(map2));
	}
	
	@Test
	public void testSimpleTypeComparison() {
		
		
		TypedObject string = new TypedObject("java.util.String");
		
		SchemaMap map = new SchemaMap(null);
		map.setKey(new TypedObjectRef(map, string));
		
		SchemaMap innerMap = new SchemaMap(map);
		map.setValue(innerMap);
		innerMap.setKey(new TypedObjectRef(innerMap, string));
		innerMap.setValue(new TypedObjectRef(innerMap, new TypedObject("java.lang.Object")));
		
		
		TypedObject string2 = new TypedObject("java.util.String");
		
		SchemaMap map2 = new SchemaMap(null);
		map2.setKey(new TypedObjectRef(map2, string2));
		
		SchemaMap innerMap2 = new SchemaMap(map2);
		map2.setValue(innerMap2);
		innerMap2.setKey(new TypedObjectRef(innerMap2, string2));
		innerMap2.setValue(new TypedObjectRef(innerMap2, new TypedObject("java.lang.Object")));
		
		Map<SchemaObject, SchemaObject> diff = new SchemaObjectComparator().compare(map, map2);
		
		Assert.assertEquals(0, diff.size());
		
		innerMap2.setValue(new TypedObjectRef(innerMap2, new TypedObject("java.lang.String")));
		diff = new SchemaObjectComparator().compare(map, map2);
		
		Assert.assertEquals(1, diff.size());
	}
	
	@Test
	public void testDeeperComparison() {
		
		TypedObject container1 = new TypedObject("Container");
		
		TypedObject type1 = new TypedObject("Type1");
		
		TypedObjectRef ref = new TypedObjectRef(container1, type1);
		ref.setName("ref");
		container1.addChild(ref);
		
		TypedObject container2 = new TypedObject("Container");
		
		TypedObject type2 = new TypedObject("Type1");
		
		ref = new TypedObjectRef(container2, type2);
		ref.setName("ref");
		container2.addChild(ref);
		
		/*
		 * insert different children into "Type1"
		 */
		ref = new TypedObjectRef(type1, new TypedObject("java.lang.String"));
		ref.setName("string");
		type1.addChild(ref);
		
		ref = new TypedObjectRef(type2, new TypedObject("java.lang.String"));
		ref.setName("str");
		type2.addChild(ref);
		
		Assert.assertTrue(new TypedObjectRef(null, container1).isEqual(new TypedObjectRef(null, container2)));
		
		Assert.assertEquals(1, new SchemaObjectComparator().compare(new TypedObjectRef(null, container1), new TypedObjectRef(null, container2)).size());
	}
}
