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
package org.jetel.data;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

import javolution.util.function.Consumer;

/**
 * @author venca (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29. 9. 2016
 */
public class RecordKeyTest extends CloverTestCase {
	
	private RecordKey normal;
	private RecordKey reversed;
	private RecordKey positionDefined;
	private RecordKey positionDefinedReversed;
	
	private DataRecordMetadata testMetadata = new DataRecordMetadata("test");
	private DataFieldMetadata fieldFoo = new DataFieldMetadata("Foo", ";");
	private DataFieldMetadata fieldBar = new DataFieldMetadata("Bar", ";");
	private DataFieldMetadata fieldBaz = new DataFieldMetadata("Baz", ";");
	
	private List<RecordKey> allTested;
	
	/**
	 * 
	 */
	public RecordKeyTest() {
		testMetadata.addField(fieldFoo);
		testMetadata.addField(fieldBar);
		testMetadata.addField(fieldBaz);
	}
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		normal=new RecordKey(new String[]{"Foo", "Bar"}, testMetadata);
		reversed=new RecordKey(new String[]{"Bar", "Foo"}, testMetadata);
		positionDefined = new RecordKey(new int[]{0, 1},testMetadata);
		positionDefinedReversed = new RecordKey(new int[] {1, 0},testMetadata);
		allTested = Arrays.asList(normal, reversed, positionDefined, positionDefinedReversed);
	}
	
	private void forAllTest(Consumer<RecordKey> test){
		for(RecordKey subject : allTested){
			test.accept(subject);
		}
	}
	
	public void testKeyfield(){
		forAllTest(new Consumer<RecordKey>() {

			@Override
			public void accept(RecordKey tested) {
				assertTrue(tested.isKeyField(0));
				assertTrue(tested.isKeyField(1));
				assertFalse(tested.isKeyField(2));
			}
		});
	}
	
	public void testLength(){
		forAllTest(new Consumer<RecordKey>() {

			@Override
			public void accept(RecordKey tested) {
				assertEquals(tested.getLength(), 2);
				
			}
		});
	}
	
	public void testNonKeyFields(){
		final int[] expected = {2};
		forAllTest(new Consumer<RecordKey>() {

			@Override
			public void accept(RecordKey tested) {
				int[] actual = tested.getNonKeyFields();
				assertTrue("Expected: "+Arrays.toString(expected)+", actual: "+Arrays.toString(actual), Arrays.equals(expected, actual));
			}
		});
	}
	
	public void testKeyFields(){
		final int[] expected = {0,1};
		forAllTest(new Consumer<RecordKey>() {

			@Override
			public void accept(RecordKey tested) {
				int[] keyFields = tested.getKeyFields();
				int[] actual = Arrays.copyOf(keyFields, keyFields.length);
				Arrays.sort(actual);
				assertTrue("Expected: "+Arrays.toString(expected)+", actual: "+Arrays.toString(actual), Arrays.equals(expected, actual));
			}
		});
	}
	
	public void testGenerateKeyRecordMetadata(){
		forAllTest(new Consumer<RecordKey>() {

			@Override
			public void accept(RecordKey tested) {
				DataRecordMetadata generated = tested.generateKeyRecordMetadata();
				assertNotNull(generated.getField("Foo"));
				assertNotNull(generated.getField("Bar"));
				assertNull(generated.getField("Baz"));
			}
		});
	}
	
	public void testGetKeyRecordMetadata(){
		forAllTest(new Consumer<RecordKey>() {

			@Override
			public void accept(RecordKey tested) {
				DataRecordMetadata generated = tested.getKeyRecordMetadata();
				assertNotNull(generated.getField("Foo"));
				assertNotNull(generated.getField("Bar"));
				assertNull(generated.getField("Baz"));
			}
		});
	}
	
	public void testEqualNulls(){
		forAllTest(new Consumer<RecordKey>() {

			@Override
			public void accept(RecordKey tested) {
				assertFalse(tested.isEqualNULLs());
				tested.setEqualNULLs(false);
				assertFalse(tested.isEqualNULLs());
				tested.setEqualNULLs(true);
				assertTrue(tested.isEqualNULLs());
				tested.setEqualNULLs(false);
				assertFalse(tested.isEqualNULLs());
			}
		});
	}
	
	public void testGetKeyFieldNames(){
		final String[] expected = {"Bar", "Foo"};
		forAllTest(new Consumer<RecordKey>() {

			@Override
			public void accept(RecordKey tested) {
				String[] keyFields = tested.getKeyFieldNames();
				String[] actual = Arrays.copyOf(keyFields, keyFields.length);
				Arrays.sort(actual);
				assertTrue("Expected: "+Arrays.toString(expected)+", actual: "+Arrays.toString(actual), Arrays.equals(expected, actual));
			}
		});
	}
	
	public void testGetReducedRecordKey(){
		forAllTest(new Consumer<RecordKey>() {

			@Override
			public void accept(RecordKey tested) {
				RecordKey reduced = tested.getReducedRecordKey();
				assertTrue(ArrayUtils.isEmpty(reduced.getNonKeyFields()));
				assertEquals(2, reduced.getKeyFields().length);
			}
		});
	}

}
