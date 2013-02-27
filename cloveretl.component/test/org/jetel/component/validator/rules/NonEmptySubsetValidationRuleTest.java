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
package org.jetel.component.validator.rules;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.common.TestDataRecordFactory;
import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.data.DataRecord;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 27.1.2013
 */
public class NonEmptySubsetValidationRuleTest extends ValidatorTestCase {

	private static final String TARGET = "target";
	private static final String TRIM = "trimInput";
	private static final String GOAL = "checkForEmptiness";
	private static final String COUNT = "count";
	@Test
	public void testNameability() {
		testNameability(NonEmptySubsetValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(NonEmptySubsetValidationRule.class);
	}
	@Test
	public void testAttributes() {
		testStringAttribute(NonEmptySubsetValidationRule.class, TARGET, "");
		testBooleanAttribute(NonEmptySubsetValidationRule.class, TRIM, false);
		testBooleanAttribute(NonEmptySubsetValidationRule.class, GOAL, false);
		testIntegerAttribute(NonEmptySubsetValidationRule.class, COUNT, 1);
	}
	@Test
	public void testReadyness() {
		AbstractValidationRule rule = new NonEmptySubsetValidationRule();
		assertFalse(rule.isReady());
		setIntegerParam(rule, COUNT, 10);
		assertFalse(rule.isReady());
		setStringParam(rule, TARGET, "target");
		assertTrue(rule.isReady());
		
		rule = new NonEmptySubsetValidationRule();
		setIntegerParam(rule, COUNT, 10);
		setStringParam(rule, TARGET, "");
		assertFalse(rule.isReady());
		
		rule = new NonEmptySubsetValidationRule();
		setIntegerParam(rule, COUNT, 10);
		setStringParam(rule, TARGET, "target,target2,target3");
		assertTrue(rule.isReady());
	}
	@Test
	public void testEmptiness() {
		DataRecord record = TestDataRecordFactory.addStringField(null, "field", "");
		record = TestDataRecordFactory.addStringField(record, "field2", "");
		record = TestDataRecordFactory.addStringField(record, "field3", "value");
		record = TestDataRecordFactory.addStringField(record, "field4", "value");
				
		assertEquals(State.VALID, createRule("field", true, 0, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field", true, 1, false).isValid(record,null));
		assertEquals(State.INVALID, createRule("field", true, 2, false).isValid(record,null));
		
		assertEquals(State.VALID, createRule("field,field2,field3,field4", true, 0, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field,field2,field3,field4", true, 1, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field,field2,field3,field4", true, 2, false).isValid(record,null));
		assertEquals(State.INVALID, createRule("field,field2,field3,field4", true, 3, false).isValid(record,null));
		
		assertEquals(State.VALID, createRule("field3,field4", true, 0, false).isValid(record,null));
		assertEquals(State.INVALID, createRule("field3,field4", true, 1, false).isValid(record,null));
		assertEquals(State.INVALID, createRule("field3,field4", true, 2, false).isValid(record,null));
	}
	@Test
	public void testNonEmptiness() {
		DataRecord record = TestDataRecordFactory.addStringField(null, "field", "value");
		record = TestDataRecordFactory.addStringField(record, "field2", "value");
		record = TestDataRecordFactory.addStringField(record, "field3", "value");
		record = TestDataRecordFactory.addStringField(record, "field4", "value");
				
		assertEquals(State.VALID, createRule("field", false, 0, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field", false, 1, false).isValid(record,null));
		assertEquals(State.INVALID, createRule("field", false, 2, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field,field2,field3", false, 0, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field,field2,field3", false, 1, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field,field2,field3", false, 2, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field,field2,field3", false, 3, false).isValid(record,null));
		assertEquals(State.INVALID, createRule("field,field2,field3", false, 4, false).isValid(record,null));
		
		record = TestDataRecordFactory.addStringField(null, "field", "value");
		record = TestDataRecordFactory.addStringField(record, "field2", "");
		record = TestDataRecordFactory.addStringField(record, "field3", "");
		record = TestDataRecordFactory.addStringField(record, "field4", "value");
		assertEquals(State.VALID, createRule("field,field2,field3,field4", false, 0, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field,field2,field3,field4", false, 1, false).isValid(record,null));
		assertEquals(State.INVALID, createRule("field,field2,field3,field4", false, 3, false).isValid(record,null));
	}
	@Test
	public void testTrimming() {
		DataRecord record = TestDataRecordFactory.addStringField(null, "field", "\n\n\n\r\tvalue");
		record = TestDataRecordFactory.addStringField(record, "field2", "    	value");
		record = TestDataRecordFactory.addStringField(record, "field3", " value   ");
		record = TestDataRecordFactory.addStringField(record, "field4", " value   ");
		record = TestDataRecordFactory.addStringField(record, "field5", " \n\t\r\n   ");
		assertEquals(State.VALID, createRule("field,field2,field3,field4", false, 4, true).isValid(record,null));
		assertEquals(State.VALID, createRule("field5", false, 1, false).isValid(record,null));
		
		record = TestDataRecordFactory.addStringField(null, "field", "\n\n\n\r\t");
		record = TestDataRecordFactory.addStringField(record, "field2", "    	");
		record = TestDataRecordFactory.addStringField(record, "field3", "       ");
		record = TestDataRecordFactory.addStringField(record, "field4", " \n\n   ");
		assertEquals(State.INVALID, createRule("field,field2,field3,field4", true, 4, false).isValid(record,null));
		assertEquals(State.VALID, createRule("field,field2,field3,field4", true, 4, true).isValid(record,null));
	}
	
	private NonEmptySubsetValidationRule createRule(String target, boolean checkForEmptiness, int count, boolean trim) {
		NonEmptySubsetValidationRule rule = new NonEmptySubsetValidationRule();
		rule.setEnabled(true);
		setStringParam(rule, TARGET, target);
		setBooleanParam(rule, GOAL, checkForEmptiness);
		setIntegerParam(rule, COUNT, count);
		setBooleanParam(rule, TRIM, trim);
		return rule;
	}
}
