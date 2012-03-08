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
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.data.tree.formatter.CollectionWriter;
import org.jetel.data.tree.formatter.TreeWriter;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @author tkramolis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6 Mar 2012
 */
public abstract class TreeWriterTest extends TreeWriterTestBase {

	protected TreeWriter treeWriter;
	
	
	public void setTreeWriter(TreeWriter treeWriter) {
		this.treeWriter = treeWriter;
	}

	
	protected static final String SIMPLE_STRUCTURE_EXPECTED_STRING = "Shoe Gazing";
	protected static final int SIMPLE_STRUCTURE_EXPECTED_INT = 10;
	
	
	public void test_simpleStructure() throws Exception {
		DataRecord record = new DataRecord(metadata);
		record.init();
		record.getField(STRING_FIELD).setValue(SIMPLE_STRUCTURE_EXPECTED_STRING);
		record.getField(INTEGER_FIELD).setValue(SIMPLE_STRUCTURE_EXPECTED_INT);

		treeWriter.writeStartTree();
		treeWriter.writeStartNode("object".toCharArray());

		treeWriter.writeStartNode("intValue".toCharArray());
		treeWriter.writeLeaf(record.getField(INTEGER_FIELD));
		treeWriter.writeEndNode("intValue".toCharArray());

		treeWriter.writeStartNode("stringValue".toCharArray());
		treeWriter.writeLeaf(record.getField(STRING_FIELD));
		treeWriter.writeEndNode("stringValue".toCharArray());

		treeWriter.writeEndNode("object".toCharArray());
		treeWriter.writeEndTree();
	}
	
	
	protected static final String[] SIMPLE_MAP_EXPECTED_STRINGS = { "Scandinavian Nu Metal", "Black metal", "Super black metal" };
	protected static final int[] SIMPLE_MAP_EXPECTED_INTS = { 20, 10, 450 };
	
	
	public void test_simpleMap() throws Exception {
		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < SIMPLE_MAP_EXPECTED_INTS.length; i++) {
			DataRecord record = new DataRecord(metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(SIMPLE_MAP_EXPECTED_STRINGS[i]);
			record.getField(INTEGER_FIELD).setValue(SIMPLE_MAP_EXPECTED_INTS[i]);
			inputData.add(record);
		}

		treeWriter.writeStartTree();
		treeWriter.writeStartNode("object".toCharArray());

		treeWriter.writeStartNode("mapStringToSimpleType".toCharArray());
		for (DataRecord rec : inputData) {
			treeWriter.writeStartNode("entry".toCharArray());

			treeWriter.writeStartNode("key".toCharArray());
			treeWriter.writeLeaf(rec.getField(STRING_FIELD));
			treeWriter.writeEndNode("key".toCharArray());

			treeWriter.writeStartNode("value".toCharArray());

			treeWriter.writeStartNode("intValue".toCharArray());
			treeWriter.writeLeaf(rec.getField(INTEGER_FIELD));
			treeWriter.writeEndNode("intValue".toCharArray());
			treeWriter.writeStartNode("stringValue".toCharArray());
			treeWriter.writeLeaf(rec.getField(STRING_FIELD));
			treeWriter.writeEndNode("stringValue".toCharArray());

			treeWriter.writeEndNode("value".toCharArray());
			treeWriter.writeEndNode("entry".toCharArray());
		}
		treeWriter.writeEndNode("mapStringToSimpleType".toCharArray());

		treeWriter.writeEndNode("object".toCharArray());
		treeWriter.writeEndTree();
	}
	

	protected static final String[] ARRAY_EXPECTED_STRINGS = { "First rule", "of", "metal", "Play it f*ckin' loud!" };
	protected static final int[] ARRAY_EXPECTED_INTS = { 20, 10, 450, 345 };

	
	public static <T extends TreeWriter & CollectionWriter> void test_array(T writer, TreeWriterTestBase test) throws Exception {
		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < ARRAY_EXPECTED_INTS.length; i++) {
			DataRecord record = new DataRecord(test.metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(ARRAY_EXPECTED_STRINGS[i]);
			record.getField(INTEGER_FIELD).setValue(ARRAY_EXPECTED_INTS[i]);
			inputData.add(record);
		}

		writer.writeStartTree();
		writer.writeStartNode("object".toCharArray());
		writer.writeStartCollection("simpleTypedValuesMatrix".toCharArray());

		for (int i = 0; i < 2; i++) {
			writer.writeStartCollection("item".toCharArray());
			for (int j = 0; j < 2; j++) {
				writer.writeStartNode("item".toCharArray());

				writer.writeStartNode("intValue".toCharArray());
				writer.writeLeaf(inputData.get((i * 2) + j).getField(INTEGER_FIELD));
				writer.writeEndNode("intValue".toCharArray());
				writer.writeStartNode("stringValue".toCharArray());
				writer.writeLeaf(inputData.get((i * 2) + j).getField(STRING_FIELD));
				writer.writeEndNode("stringValue".toCharArray());

				writer.writeEndNode("item".toCharArray());
			}
			writer.writeEndCollection("item".toCharArray());
		}

		writer.writeEndCollection("simpleTypedValuesMatrix".toCharArray());
		writer.writeEndNode("object".toCharArray());
		writer.writeEndTree();
	}

	
	protected static final String[] COLLECTION_EXPECTED_STRINGS = { "Buddy you’re", " a boy make a ", "big noise", "Playin’ in the street", "gonna be a big", " man some day" };
	protected static final int[] COLLECTION_EXPECTED_INTS = { 20, 10, 450, 345, 435, 324245 };
	protected static final double[] COLLECTION_EXPECTED_DOUBLES = { 204534534l, 1345345340l, 450l, 3452342l, -23435l, 234l };
	
	
	public static <T extends TreeWriter & CollectionWriter> void test_collectionInBean(T writer, TreeWriterTestBase test) throws Exception {
		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < COLLECTION_EXPECTED_INTS.length; i++) {
			DataRecord record = new DataRecord(test.metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(COLLECTION_EXPECTED_STRINGS[i]);
			record.getField(INTEGER_FIELD).setValue(COLLECTION_EXPECTED_INTS[i]);
			record.getField(LONG_FIELD).setValue(COLLECTION_EXPECTED_DOUBLES[i]);
			inputData.add(record);
		}

		writer.writeStartTree();
		writer.writeStartNode("object".toCharArray());
		writer.writeStartCollection("mapList".toCharArray());

		for (int i = 0; i < 3; i++) {
			writer.writeStartNode("item".toCharArray());
			for (int j = 0; j < 2; j++) {
				int index = (i * 2) + j;
				writer.writeStartNode("entry".toCharArray());

				writer.writeStartNode("key".toCharArray());
				writer.writeStartNode("intValue".toCharArray());
				writer.writeLeaf(inputData.get(index).getField(INTEGER_FIELD));
				writer.writeEndNode("intValue".toCharArray());
				writer.writeStartNode("stringValue".toCharArray());
				writer.writeLeaf(inputData.get(index).getField(STRING_FIELD));
				writer.writeEndNode("stringValue".toCharArray());
				writer.writeEndNode("key".toCharArray());

				writer.writeStartNode("value".toCharArray());
				writer.writeLeaf(inputData.get(index).getField(LONG_FIELD));
				writer.writeEndNode("value".toCharArray());
				writer.writeEndNode("entry".toCharArray());
			}
			writer.writeEndNode("item".toCharArray());
		}

		writer.writeEndCollection("mapList".toCharArray());
		writer.writeEndNode("object".toCharArray());
		writer.writeEndTree();
	}

	
	protected static final String[] LIST_EXPECTED_STRINGS = { "Buddy ", "you’re a", " young man", " hard man", "Shoutin’ in", " the street gonna", " take on the", " world some day", "You got blood", " on yo’ face", "You big", " disgrace", "Wavin’ ", "your banner", " all over", " the place" };
	protected static final String[] LIST_EXPECTED_KEYS = { "We will,", " we will", " rock", " you" };
	
	public static <T extends TreeWriter & CollectionWriter> void test_list(T writer, TreeWriterTestBase test) throws Exception {
		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < LIST_EXPECTED_STRINGS.length; i++) {
			DataRecord record = new DataRecord(test.metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(LIST_EXPECTED_STRINGS[i]);
			inputData.add(record);
		}

		writer.writeStartTree();
		writer.writeStartCollection("list".toCharArray());

		for (int i = 0; i < 2; i++) {
			writer.writeStartNode("item".toCharArray());
			for (int j = 0; j < 2; j++) {
				int index = (i * 2) + j;
				writer.writeStartNode("entry".toCharArray());

				writer.writeStartNode("key".toCharArray());
				writer.writeLeaf(LIST_EXPECTED_KEYS[index]);
				writer.writeEndNode("key".toCharArray());

				writer.writeStartNode("value".toCharArray());
				for (int h = 0; h < 2; h++) {
					int inIndex = (i * 8) + (j * 4) + (h * 2);
					writer.writeStartNode("entry".toCharArray());
					writer.writeStartNode("key".toCharArray());
					writer.writeLeaf(inputData.get(inIndex).getField(STRING_FIELD));
					writer.writeEndNode("key".toCharArray());
					writer.writeStartNode("value".toCharArray());
					writer.writeLeaf(inputData.get(inIndex + 1).getField(STRING_FIELD));
					writer.writeEndNode("value".toCharArray());
					writer.writeEndNode("entry".toCharArray());
				}
				writer.writeEndNode("value".toCharArray());
				writer.writeEndNode("entry".toCharArray());
			}
			writer.writeEndNode("item".toCharArray());
		}
		writer.writeEndCollection("list".toCharArray());
		writer.writeEndTree();
	}


	protected static final String[] LIST_OF_BEANS_EXPECTED_STRINGS = { "Dobra rada", "do zivota:", "\"Nejez", "zluty", "snih\"" };
	
	public static <T extends TreeWriter & CollectionWriter> void test_list_of_beans(T writer, TreeWriterTestBase test) throws Exception {
		List<DataRecord> inputData = new ArrayList<DataRecord>();
		
		for (int i = 0; i < LIST_OF_BEANS_EXPECTED_STRINGS.length; i++) {
			DataRecord record = new DataRecord(test.metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(LIST_OF_BEANS_EXPECTED_STRINGS[i]);
			inputData.add(record);
		}
		
		writer.writeStartTree();
		writer.writeStartCollection("list".toCharArray());
		
		for (int i = 0; i < LIST_OF_BEANS_EXPECTED_STRINGS.length; i++) {
			writer.writeStartNode("item".toCharArray());
			writer.writeStartNode("stringValue".toCharArray());
			writer.writeLeaf(LIST_OF_BEANS_EXPECTED_STRINGS[i]);
			writer.writeEndNode("stringValue".toCharArray());
			writer.writeEndNode("item".toCharArray());
		}
		
		writer.writeEndCollection("list".toCharArray());
		writer.writeEndTree();
	}

	
	protected static final String[] LIST_OF_PROPERTIES_EXPECTED_STRINGS = { "Dobra rada", "do zivota:", "\"Nejez", "zluty", "snih\"" };
	
	public static <T extends TreeWriter & CollectionWriter> void test_list_of_properties(T writer, TreeWriterTestBase test) throws Exception {
		List<DataRecord> inputData = new ArrayList<DataRecord>();

		for (int i = 0; i < LIST_OF_PROPERTIES_EXPECTED_STRINGS.length; i++) {
			DataRecord record = new DataRecord(test.metadata);
			record.init();
			record.getField(STRING_FIELD).setValue(LIST_OF_PROPERTIES_EXPECTED_STRINGS[i]);
			inputData.add(record);
		}

		writer.writeStartTree();
		writer.writeStartCollection("list".toCharArray());

		for (int i = 0; i < LIST_OF_PROPERTIES_EXPECTED_STRINGS.length; i++) {
			writer.writeStartNode("item".toCharArray());
			writer.writeLeaf(LIST_OF_PROPERTIES_EXPECTED_STRINGS[i]);
			writer.writeEndNode("item".toCharArray());
		}

		writer.writeEndCollection("list".toCharArray());
		writer.writeEndTree();
	}

}
