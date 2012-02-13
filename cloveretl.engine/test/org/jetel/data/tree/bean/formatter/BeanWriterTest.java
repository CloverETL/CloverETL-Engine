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

import org.jetel.data.DataRecord;
import org.jetel.data.tree.bean.ComplexTestType;
import org.jetel.data.tree.bean.ComplexTestTypeWithArray;
import org.jetel.data.tree.bean.ComplexTestTypeWithCollection;
import org.jetel.data.tree.bean.ListOfMapsOfMaps;
import org.jetel.data.tree.bean.SimpleTestType;
import org.jetel.data.tree.bean.formatter.BeanWriter;
import org.jetel.data.tree.bean.schema.generator.BeanParser;
import org.jetel.data.tree.bean.schema.model.SchemaCollection;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 7.11.2011
 */
public class BeanWriterTest extends CloverTestCase {

	private static final String BOOLEAN_FIELD = "booleanField";
	private static final String BYTE_FIELD = "byteField";
	private static final String INTEGER_FIELD = "intField";
	private static final String LONG_FIELD = "longField";
	private static final String DECIMAL_FIELD = "decimalField";
	private static final String DATE_FIELD = "dateField";
	private static final String NUMERIC_FIELD = "numericField";
	private static final String STRING_FIELD = "stringField";

	private DataRecordMetadata metadata;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
		metadata = new DataRecordMetadata("meta", DataRecordMetadata.DELIMITED_RECORD);
		metadata.setFieldDelimiter("\n");
		metadata.setRecordDelimiter("\n");
		metadata.addField(new DataFieldMetadata(BOOLEAN_FIELD, DataFieldMetadata.BOOLEAN_FIELD, "|"));
		metadata.addField(new DataFieldMetadata(BYTE_FIELD, DataFieldMetadata.BYTE_FIELD, "|"));
		metadata.addField(new DataFieldMetadata(INTEGER_FIELD, DataFieldMetadata.INTEGER_FIELD, "|"));
		metadata.addField(new DataFieldMetadata(LONG_FIELD, DataFieldMetadata.LONG_FIELD, "|"));
		metadata.addField(new DataFieldMetadata(DECIMAL_FIELD, DataFieldMetadata.DECIMAL_FIELD, "|"));
		metadata.addField(new DataFieldMetadata(DATE_FIELD, DataFieldMetadata.DATE_FIELD, "|"));
		metadata.addField(new DataFieldMetadata(NUMERIC_FIELD, DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata.addField(new DataFieldMetadata(STRING_FIELD, DataFieldMetadata.STRING_FIELD, "|"));
	}

	public void test_simpleStructure() throws Exception {
		String expectedString = "Shoe Gazing";
		int expectedInt = 10;

		DataRecord record = new DataRecord(metadata);
		record.init();
		record.getField(STRING_FIELD).setValue(expectedString);
		record.getField(INTEGER_FIELD).setValue(expectedInt);

		SimpleTestType expected = new SimpleTestType();
		expected.setIntValue(expectedInt);
		expected.setStringValue(expectedString);

		SchemaObject structure = BeanParser.parse(SimpleTestType.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		beanWriter.writeStartTree();
		beanWriter.writeStartNode("object".toCharArray());

		beanWriter.writeStartNode("intValue".toCharArray());
		beanWriter.writeLeaf(record.getField(INTEGER_FIELD));
		beanWriter.writeEndNode("intValue".toCharArray());

		beanWriter.writeStartNode("stringValue".toCharArray());
		beanWriter.writeLeaf(record.getField(STRING_FIELD));
		beanWriter.writeEndNode("stringValue".toCharArray());

		beanWriter.writeEndNode("object".toCharArray());
		beanWriter.writeEndTree();

		SimpleTestType result = (SimpleTestType) beanWriter.flushBean();
		assertEquals(expected, result);
	}

	public void test_simpleMap() throws Exception {
		String[] expectedStrings = { "Scandinavian Nu Metal", "Black metal", "Super black metal" };
		int[] expectedInts = { 20, 10, 450 };

		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < expectedInts.length; i++) {
			DataRecord record = new DataRecord(metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(expectedStrings[i]);
			record.getField(INTEGER_FIELD).setValue(expectedInts[i]);
			inputData.add(record);
		}

		Map<String, SimpleTestType> mapStringToSimple = new LinkedHashMap<String, SimpleTestType>();

		for (int i = 0; i < expectedInts.length; i++) {
			SimpleTestType simple = new SimpleTestType();
			simple.setIntValue(expectedInts[i]);
			simple.setStringValue(expectedStrings[i]);
			mapStringToSimple.put(expectedStrings[i], simple);
		}

		ComplexTestType expected = new ComplexTestType();
		expected.setMapStringToSimpleType(mapStringToSimple);

		SchemaObject structure = BeanParser.parse(ComplexTestType.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		beanWriter.writeStartTree();
		beanWriter.writeStartNode("object".toCharArray());

		beanWriter.writeStartNode("mapStringToSimpleType".toCharArray());
		for (DataRecord rec : inputData) {
			beanWriter.writeStartNode("entry".toCharArray());

			beanWriter.writeStartNode("key".toCharArray());
			beanWriter.writeLeaf(rec.getField(STRING_FIELD));
			beanWriter.writeEndNode("key".toCharArray());

			beanWriter.writeStartNode("value".toCharArray());

			beanWriter.writeStartNode("intValue".toCharArray());
			beanWriter.writeLeaf(rec.getField(INTEGER_FIELD));
			beanWriter.writeEndNode("intValue".toCharArray());
			beanWriter.writeStartNode("stringValue".toCharArray());
			beanWriter.writeLeaf(rec.getField(STRING_FIELD));
			beanWriter.writeEndNode("stringValue".toCharArray());

			beanWriter.writeEndNode("value".toCharArray());
			beanWriter.writeEndNode("entry".toCharArray());
		}
		beanWriter.writeEndNode("mapStringToSimpleType".toCharArray());

		beanWriter.writeEndNode("object".toCharArray());
		beanWriter.writeEndTree();

		ComplexTestType result = (ComplexTestType) beanWriter.flushBean();
		assertEquals(expected, result);
	}

	public void test_arrayInBean() throws Exception {
		String[] expectedStrings = { "First rule", "of", "metal", "Play it f*ckin' loud!" };
		int[] expectedInts = { 20, 10, 450, 345 };

		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < expectedInts.length; i++) {
			DataRecord record = new DataRecord(metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(expectedStrings[i]);
			record.getField(INTEGER_FIELD).setValue(expectedInts[i]);
			inputData.add(record);
		}

		SimpleTestType[][] expectedMatrix = new SimpleTestType[2][2];

		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < 2; j++) {
				SimpleTestType simple = new SimpleTestType();
				simple.setIntValue(expectedInts[(i * 2) + j]);
				simple.setStringValue(expectedStrings[(i * 2) + j]);
				expectedMatrix[i][j] = simple;
			}
		}

		ComplexTestTypeWithArray expected = new ComplexTestTypeWithArray();
		expected.setSimpleTypedValuesMatrix(expectedMatrix);

		SchemaObject structure = BeanParser.parse(ComplexTestTypeWithArray.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		beanWriter.writeStartTree();
		beanWriter.writeStartNode("object".toCharArray());

		for (int i = 0; i < 2; i++) {
			beanWriter.writeStartNode("simpleTypedValuesMatrix".toCharArray());
			for (int j = 0; j < 2; j++) {
				beanWriter.writeStartNode("item".toCharArray());

				beanWriter.writeStartNode("intValue".toCharArray());
				beanWriter.writeLeaf(inputData.get((i * 2) + j).getField(INTEGER_FIELD));
				beanWriter.writeEndNode("intValue".toCharArray());
				beanWriter.writeStartNode("stringValue".toCharArray());
				beanWriter.writeLeaf(inputData.get((i * 2) + j).getField(STRING_FIELD));
				beanWriter.writeEndNode("stringValue".toCharArray());

				beanWriter.writeEndNode("item".toCharArray());
			}
			beanWriter.writeEndNode("simpleTypedValuesMatrix".toCharArray());
		}

		beanWriter.writeEndNode("object".toCharArray());
		beanWriter.writeEndTree();

		ComplexTestTypeWithArray result = (ComplexTestTypeWithArray) beanWriter.flushBean();
		assertEquals(expected, result);
	}

	public void test_collectionInBean() throws Exception {
		String[] expectedStrings = { "Buddy you’re", " a boy make a ", "big noise", "Playin’ in the street", "gonna be a big", " man some day" };
		int[] expectedInts = { 20, 10, 450, 345, 435, 324245 };
		double[] expectedDoubles = { 204534534l, 1345345340l, 450l, 3452342l, -23435l, 234l };

		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < expectedInts.length; i++) {
			DataRecord record = new DataRecord(metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(expectedStrings[i]);
			record.getField(INTEGER_FIELD).setValue(expectedInts[i]);
			record.getField(LONG_FIELD).setValue(expectedDoubles[i]);
			inputData.add(record);
		}

		List<Map<SimpleTestType, Double>> expectedCollection = new LinkedList<Map<SimpleTestType, Double>>();
		for (int i = 0; i < 3; i++) {
			Map<SimpleTestType, Double> expectedMap = new HashMap<SimpleTestType, Double>();
			for (int j = 0; j < 2; j++) {
				int index = (i * 2) + j;
				SimpleTestType simple = new SimpleTestType();
				simple.setIntValue(expectedInts[index]);
				simple.setStringValue(expectedStrings[index]);
				expectedMap.put(simple, expectedDoubles[index]);
			}
			expectedCollection.add(expectedMap);
		}

		ComplexTestTypeWithCollection expected = new ComplexTestTypeWithCollection();
		expected.setMapList(expectedCollection);

		SchemaObject structure = BeanParser.parse(ComplexTestTypeWithCollection.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		beanWriter.writeStartTree();
		beanWriter.writeStartNode("object".toCharArray());

		for (int i = 0; i < 3; i++) {
			beanWriter.writeStartNode("mapList".toCharArray());
			for (int j = 0; j < 2; j++) {
				int index = (i * 2) + j;
				beanWriter.writeStartNode("entry".toCharArray());

				beanWriter.writeStartNode("key".toCharArray());
				beanWriter.writeStartNode("intValue".toCharArray());
				beanWriter.writeLeaf(inputData.get(index).getField(INTEGER_FIELD));
				beanWriter.writeEndNode("intValue".toCharArray());
				beanWriter.writeStartNode("stringValue".toCharArray());
				beanWriter.writeLeaf(inputData.get(index).getField(STRING_FIELD));
				beanWriter.writeEndNode("stringValue".toCharArray());
				beanWriter.writeEndNode("key".toCharArray());

				beanWriter.writeStartNode("value".toCharArray());
				beanWriter.writeLeaf(inputData.get(index).getField(LONG_FIELD));
				beanWriter.writeEndNode("value".toCharArray());
				beanWriter.writeEndNode("entry".toCharArray());
			}
			beanWriter.writeEndNode("mapList".toCharArray());
		}

		beanWriter.writeEndNode("object".toCharArray());
		beanWriter.writeEndTree();

		ComplexTestTypeWithCollection result = (ComplexTestTypeWithCollection) beanWriter.flushBean();
		assertEquals(expected, result);
	}

	public void test_list() throws Exception {
		String[] expectedStrings = { "Buddy ", "you’re a", " young man", " hard man", "Shoutin’ in", " the street gonna", " take on the", " world some day", "You got blood", " on yo’ face", "You big", " disgrace", "Wavin’ ", "your banner", " all over", " the place" };
		String[] expectedKeys = { "We will,", " we will", " rock", " you" };

		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < expectedStrings.length; i++) {
			DataRecord record = new DataRecord(metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(expectedStrings[i]);
			inputData.add(record);
		}

		ListOfMapsOfMaps expected = new ListOfMapsOfMaps();
		for (int i = 0; i < 2; i++) {
			Map<String, HashMap<String, String>> expectedOuterMap = new HashMap<String, HashMap<String, String>>();
			for (int j = 0; j < 2; j++) {
				HashMap<String, String> expectedInnerMap = new HashMap<String, String>();
				for (int h = 0; h < 2; h++) {
					int index = (i * 8) + (j * 4) + (h * 2);
					expectedInnerMap.put(expectedStrings[index], expectedStrings[index + 1]);
				}
				expectedOuterMap.put(expectedKeys[(i * 2) + j], expectedInnerMap);
			}
			expected.add(expectedOuterMap);
		}

		SchemaObject structure = BeanParser.parse(ListOfMapsOfMaps.class);
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		beanWriter.writeStartTree();

		for (int i = 0; i < 2; i++) {
			beanWriter.writeStartNode("list".toCharArray());
			for (int j = 0; j < 2; j++) {
				int index = (i * 2) + j;
				beanWriter.writeStartNode("entry".toCharArray());

				beanWriter.writeStartNode("key".toCharArray());
				beanWriter.writeLeaf(expectedKeys[index]);
				beanWriter.writeEndNode("key".toCharArray());

				beanWriter.writeStartNode("value".toCharArray());
				for (int h = 0; h < 2; h++) {
					int inIndex = (i * 8) + (j * 4) + (h * 2);
					beanWriter.writeStartNode("entry".toCharArray());
					beanWriter.writeStartNode("key".toCharArray());
					beanWriter.writeLeaf(inputData.get(inIndex).getField(STRING_FIELD));
					beanWriter.writeEndNode("key".toCharArray());
					beanWriter.writeStartNode("value".toCharArray());
					beanWriter.writeLeaf(inputData.get(inIndex + 1).getField(STRING_FIELD));
					beanWriter.writeEndNode("value".toCharArray());
					beanWriter.writeEndNode("entry".toCharArray());
				}
				beanWriter.writeEndNode("value".toCharArray());
				beanWriter.writeEndNode("entry".toCharArray());
			}
			beanWriter.writeEndNode("list".toCharArray());
		}
		beanWriter.writeEndTree();

		ListOfMapsOfMaps result = (ListOfMapsOfMaps) beanWriter.flushBean();
		assertEquals(expected, result);
	}

	@SuppressWarnings("unchecked")
	public void test_list_of_beans() throws Exception {
		String[] expectedStrings = { "Dobra rada", "do zivota:", "\"Nejez", "zluty", "snih\"" };
		
		List<DataRecord> inputData = new ArrayList<DataRecord>();
		
		for (int i = 0; i < expectedStrings.length; i++) {
			DataRecord record = new DataRecord(metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(expectedStrings[i]);
			inputData.add(record);
		}
		
		List<SimpleTestType> expected = new ArrayList<SimpleTestType>();
		for (int i = 0; i < expectedStrings.length; i++) {
			SimpleTestType item = new SimpleTestType();
			item.setStringValue(expectedStrings[i]);
			expected.add(item);
		}
		
		SchemaCollection structure = new SchemaCollection(null);
		structure.setItem(new TypedObjectRef(structure, new TypedObject(SimpleTestType.class.getName())));
		structure.setType(ArrayList.class.getName());
		structure = BeanParser.addReferencedTypes(structure, getClass().getClassLoader());
		
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());
		
		beanWriter.writeStartTree();
		
		for (int i = 0; i < expectedStrings.length; i++) {
			beanWriter.writeStartNode("list".toCharArray());
			beanWriter.writeStartNode("stringValue".toCharArray());
			beanWriter.writeLeaf(expectedStrings[i]);
			beanWriter.writeEndNode("stringValue".toCharArray());
			beanWriter.writeEndNode("list".toCharArray());
		}
		beanWriter.writeEndTree();
		
		List<SimpleTestType> result = (List<SimpleTestType>) beanWriter.flushBean();
		assertEquals(expected, result);
	}
	
	@SuppressWarnings("unchecked")
	public void test_list_of_properties() throws Exception {
		String[] expectedStrings = { "Dobra rada", "do zivota:", "\"Nejez", "zluty", "snih\"" };

		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < expectedStrings.length; i++) {
			DataRecord record = new DataRecord(metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(expectedStrings[i]);
			inputData.add(record);
		}

		List<String> expected = Arrays.asList(expectedStrings);
		SchemaCollection structure = new SchemaCollection(null);
		structure.setItem(new TypedObjectRef(structure, new TypedObject(String.class.getName())));
		structure.setType(ArrayList.class.getName());
		structure = BeanParser.addReferencedTypes(structure, getClass().getClassLoader());
		
		BeanWriter beanWriter = new BeanWriter(structure, Thread.currentThread().getContextClassLoader());

		beanWriter.writeStartTree();

		for (int i = 0; i < expectedStrings.length; i++) {
			beanWriter.writeStartNode("list".toCharArray());
			beanWriter.writeLeaf(expectedStrings[i]);
			beanWriter.writeEndNode("list".toCharArray());
		}
		beanWriter.writeEndTree();

		List<String> result = (List<String>) beanWriter.flushBean();
		assertEquals(expected, result);
	}

}
