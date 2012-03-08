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
package org.jetel.data.tree.bean.formatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jetel.data.tree.bean.ComplexTestType;
import org.jetel.data.tree.bean.ComplexTestTypeWithArray;
import org.jetel.data.tree.bean.ComplexTestTypeWithCollection;
import org.jetel.data.tree.bean.ListOfMapsOfMaps;
import org.jetel.data.tree.bean.SimpleTestType;
import org.jetel.data.tree.bean.schema.generator.BeanParser;
import org.jetel.data.tree.bean.schema.model.SchemaCollection;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 7.11.2011
 */
public class BeanWriterTest extends SampleTreeWriter {

	public void test_simpleStructure() throws Exception {
		SimpleTestType expected = new SimpleTestType();
		expected.setIntValue(SIMPLE_STRUCTURE_EXPECTED_INT);
		expected.setStringValue(SIMPLE_STRUCTURE_EXPECTED_STRING);

		SchemaObject structure = BeanParser.parse(SimpleTestType.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		writeSimpleStructure(beanWriter, this);
		
		SimpleTestType result = (SimpleTestType) beanWriter.flushBean();
		assertEquals(expected, result);
	}


	public void test_simpleMap() throws Exception {
		Map<String, SimpleTestType> mapStringToSimple = new LinkedHashMap<String, SimpleTestType>();

		for (int i = 0; i < SIMPLE_MAP_EXPECTED_INTS.length; i++) {
			SimpleTestType simple = new SimpleTestType();
			simple.setIntValue(SIMPLE_MAP_EXPECTED_INTS[i]);
			simple.setStringValue(SIMPLE_MAP_EXPECTED_STRINGS[i]);
			mapStringToSimple.put(SIMPLE_MAP_EXPECTED_STRINGS[i], simple);
		}

		ComplexTestType expected = new ComplexTestType();
		expected.setMapStringToSimpleType(mapStringToSimple);

		SchemaObject structure = BeanParser.parse(ComplexTestType.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		writeSimpleMap(beanWriter, this);
		
		ComplexTestType result = (ComplexTestType) beanWriter.flushBean();
		assertEquals(expected, result);
	}

	public void test_array() throws Exception {
		SimpleTestType[][] expectedMatrix = new SimpleTestType[2][2];

		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < 2; j++) {
				SimpleTestType simple = new SimpleTestType();
				simple.setIntValue(ARRAY_EXPECTED_INTS[(i * 2) + j]);
				simple.setStringValue(ARRAY_EXPECTED_STRINGS[(i * 2) + j]);
				expectedMatrix[i][j] = simple;
			}
		}

		ComplexTestTypeWithArray expected = new ComplexTestTypeWithArray();
		expected.setSimpleTypedValuesMatrix(expectedMatrix);

		SchemaObject structure = BeanParser.parse(ComplexTestTypeWithArray.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		test_array(beanWriter, this);
		
		ComplexTestTypeWithArray result = (ComplexTestTypeWithArray) beanWriter.flushBean();
		assertEquals(expected, result);
	}

	public void test_collectionInBean() throws Exception {
		List<Map<SimpleTestType, Double>> expectedCollection = new LinkedList<Map<SimpleTestType, Double>>();
		for (int i = 0; i < 3; i++) {
			Map<SimpleTestType, Double> expectedMap = new HashMap<SimpleTestType, Double>();
			for (int j = 0; j < 2; j++) {
				int index = (i * 2) + j;
				SimpleTestType simple = new SimpleTestType();
				simple.setIntValue(COLLECTION_EXPECTED_INTS[index]);
				simple.setStringValue(COLLECTION_EXPECTED_STRINGS[index]);
				expectedMap.put(simple, COLLECTION_EXPECTED_DOUBLES[index]);
			}
			expectedCollection.add(expectedMap);
		}

		ComplexTestTypeWithCollection expected = new ComplexTestTypeWithCollection();
		expected.setMapList(expectedCollection);

		SchemaObject structure = BeanParser.parse(ComplexTestTypeWithCollection.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		test_collectionInBean(beanWriter, this);
		
		ComplexTestTypeWithCollection result = (ComplexTestTypeWithCollection) beanWriter.flushBean();
		assertEquals(expected, result);
	}

	public void test_list() throws Exception {
		ListOfMapsOfMaps expected = new ListOfMapsOfMaps();
		for (int i = 0; i < 2; i++) {
			Map<String, HashMap<String, String>> expectedOuterMap = new HashMap<String, HashMap<String, String>>();
			for (int j = 0; j < 2; j++) {
				HashMap<String, String> expectedInnerMap = new HashMap<String, String>();
				for (int h = 0; h < 2; h++) {
					int index = (i * 8) + (j * 4) + (h * 2);
					expectedInnerMap.put(LIST_EXPECTED_STRINGS[index], LIST_EXPECTED_STRINGS[index + 1]);
				}
				expectedOuterMap.put(LIST_EXPECTED_KEYS[(i * 2) + j], expectedInnerMap);
			}
			expected.add(expectedOuterMap);
		}

		SchemaObject structure = BeanParser.parse(ListOfMapsOfMaps.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		test_list(beanWriter, this);

		ListOfMapsOfMaps result = (ListOfMapsOfMaps) beanWriter.flushBean();
		assertEquals(expected, result);
	}

	@SuppressWarnings("unchecked")
	public void test_list_of_beans() throws Exception {
		List<SimpleTestType> expected = new ArrayList<SimpleTestType>();
		for (int i = 0; i < LIST_OF_BEANS_EXPECTED_STRINGS.length; i++) {
			SimpleTestType item = new SimpleTestType();
			item.setStringValue(LIST_OF_BEANS_EXPECTED_STRINGS[i]);
			expected.add(item);
		}
		
		SchemaCollection structure = new SchemaCollection(null);
		structure.setItem(new TypedObjectRef(structure, new TypedObject(SimpleTestType.class.getName())));
		structure.setType(ArrayList.class.getName());
		structure = BeanParser.addReferencedTypes(structure, getClass().getClassLoader());
		
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		test_list_of_beans(beanWriter, this);
		
		List<SimpleTestType> result = (List<SimpleTestType>) beanWriter.flushBean();
		assertEquals(expected, result);
	}
	
	@SuppressWarnings("unchecked")
	public void test_list_of_properties() throws Exception {
		List<String> expected = Arrays.asList(LIST_OF_PROPERTIES_EXPECTED_STRINGS);
		SchemaCollection structure = new SchemaCollection(null);
		structure.setItem(new TypedObjectRef(structure, new TypedObject(String.class.getName())));
		structure.setType(ArrayList.class.getName());
		structure = BeanParser.addReferencedTypes(structure, getClass().getClassLoader());
		
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		test_list_of_properties(beanWriter, this);

		List<String> result = (List<String>) beanWriter.flushBean();
		assertEquals(expected, result);
	}


}
