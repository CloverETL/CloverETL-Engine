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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.jetel.data.tree.json.formatter.JsonWriterAdapter;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @author tkramolis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Mar 2012
 */
public class JsonWriterAdapterTest extends TreeWriterTest {

	private static final String ENCODING = "UTF-8";
	private final ByteArrayOutputStream outStream = new ByteArrayOutputStream(1024);
	private JsonWriterAdapter jsonWriter;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		outStream.reset();
		jsonWriter = new JsonWriterAdapter(outStream, ENCODING, true);
	}
	
	public void test_simpleStructure() throws Exception {
		setTreeWriter(jsonWriter);
		
		super.test_simpleStructure();
		
		String expected = "{"+simpleIntField(SIMPLE_STRUCTURE_EXPECTED_INT)+","+simpleStringField(SIMPLE_STRUCTURE_EXPECTED_STRING)+"}";
		cleanupAndResultCheck(expected);
	}

	private static String simpleIntField(int value) {
		return "\"intValue\":" + value;
	}
	
	private static String simpleStringField(String value) {
		return "\"stringValue\":\"" + value + "\"";
	}
	
	private void cleanupAndResultCheck(String expectedResult) throws IOException, UnsupportedEncodingException {
		jsonWriter.flush();
		jsonWriter.close();

		String result = outStream.toString(ENCODING);

		System.out.println("--- expected ---\n" + expectedResult);
		System.out.println("--- actual ---\n" + result);

		assertEquals(expectedResult, result);
	}
	
	public void test_simpleMap() throws Exception {
		StringBuilder sb = new StringBuilder("{\"mapStringToSimpleType\":{");
		
		for (int i = 0; i < SIMPLE_MAP_EXPECTED_INTS.length; i++) {
			sb.append("\"entry\":{");
			sb.append("\"key\":\"").append(SIMPLE_MAP_EXPECTED_STRINGS[i]).append("\",");
			sb.append("\"value\":{");
			sb.append(simpleIntField(SIMPLE_MAP_EXPECTED_INTS[i])).append(",");
			sb.append(simpleStringField(SIMPLE_MAP_EXPECTED_STRINGS[i])).append("}");					
			sb.append("},");
		}
		sb.setLength(sb.length() - 1); // remove extra ','
		
		sb.append("}}");

		setTreeWriter(jsonWriter);

		super.test_simpleMap();
	
		cleanupAndResultCheck(sb.toString());
	}

	public void test_array() throws Exception {
		StringBuilder sb = new StringBuilder("{\"simpleTypedValuesMatrix\":[");
		
		for (int i = 0; i < 2; i++) {
			sb.append("[");
			for (int j = 0; j < 2; j++) {
				sb.append("{");
				sb.append(simpleIntField(ARRAY_EXPECTED_INTS[(i * 2) + j])).append(",");
				sb.append(simpleStringField(ARRAY_EXPECTED_STRINGS[(i * 2) + j]));
				sb.append("},");
			}
			sb.setLength(sb.length() - 1);
			sb.append("],");
		}
		sb.setLength(sb.length() - 1);
		
		sb.append("]}");

		test_array(jsonWriter, this);

		cleanupAndResultCheck(sb.toString());
	}

	public void test_list() throws Exception {
		StringBuilder sb = new StringBuilder("[");

		for (int i = 0; i < 2; i++) {
			sb.append("{");
			for (int j = 0; j < 2; j++) {
				int index = (i * 2) + j;
				sb.append("\"entry\":{");
				sb.append("\"key\":\"").append(LIST_EXPECTED_KEYS[index]).append("\",");
				sb.append("\"value\":{");
				for (int h = 0; h < 2; h++) {
					int inIndex = (i * 8) + (j * 4) + (h * 2);
					sb.append("\"entry\":{");
					sb.append("\"key\":\"").append(LIST_EXPECTED_STRINGS[inIndex]).append("\",");
					sb.append("\"value\":\"").append(LIST_EXPECTED_STRINGS[inIndex + 1]).append("\"");
					sb.append("},");
				}
				sb.setLength(sb.length() - 1);
				sb.append("}},");
			}
			sb.setLength(sb.length() - 1);
			sb.append("},");
		}
		sb.setLength(sb.length() - 1);
		sb.append("]");

		test_list(jsonWriter, this);
		
		cleanupAndResultCheck(sb.toString());
	}

	public void test_list_of_beans() throws Exception {
		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < LIST_OF_BEANS_EXPECTED_STRINGS.length; i++) {
			sb.append("{");
			sb.append("\"stringValue\":\"").append(escape(LIST_OF_BEANS_EXPECTED_STRINGS[i])).append("\"");
			sb.append("},");
		}
		sb.setLength(sb.length() - 1);
		sb.append("]");

		test_list_of_beans(jsonWriter, this);
		
		cleanupAndResultCheck(sb.toString());
	}
	
	private static String escape(String string) {
		return string.replaceAll("([\"\\\\])", "\\\\$0"); // also control characters are not allowed...
	}
	
	public void test_list_of_properties() throws Exception {
		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < LIST_OF_PROPERTIES_EXPECTED_STRINGS.length; i++) {
			sb.append("\"").append(escape(LIST_OF_PROPERTIES_EXPECTED_STRINGS[i])).append("\",");
		}
		sb.setLength(sb.length() - 1);
		sb.append("]");

		test_list_of_properties(jsonWriter, this);
		
		cleanupAndResultCheck(sb.toString());
	}

}
