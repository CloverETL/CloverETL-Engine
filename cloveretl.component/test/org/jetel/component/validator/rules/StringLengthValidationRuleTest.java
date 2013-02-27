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
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 27.1.2013
 */
public class StringLengthValidationRuleTest extends ValidatorTestCase {
	
	private static final String TARGET = "target";
	private static final String TRIM = "trimInput";
	private static final String FROM = "from";
	private static final String TO = "to";
	private static final String TYPE = "type";

	@Test
	public void testNameability() {
		testNameability(StringLengthValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(StringLengthValidationRule.class);
	}
	@Test
	public void testAttributes() {
		testStringAttribute(StringLengthValidationRule.class, TARGET, "");
		testBooleanAttribute(StringLengthValidationRule.class, TRIM, false);
		testIntegerAttribute(StringLengthValidationRule.class, FROM, null);
		testIntegerAttribute(StringLengthValidationRule.class, TO, null);
		testEnumAttribute(StringLengthValidationRule.class, TYPE, StringLengthValidationRule.TYPES.values(), StringLengthValidationRule.TYPES.EXACT);
	}
	@Test
	public void testReadyness() {
		AbstractValidationRule rule = new StringLengthValidationRule();
		assertFalse(rule.isReady());
		
		rule = new StringLengthValidationRule();
		setStringParam(rule, TARGET, "some text");
		assertFalse(rule.isReady());
		
		rule = new StringLengthValidationRule();
		setStringParam(rule, TARGET, "some text");
		setEnumParam(rule, TYPE, StringLengthValidationRule.TYPES.EXACT);
		assertFalse(rule.isReady());
		setIntegerParam(rule, TO, 20);
		assertFalse(rule.isReady());
		setIntegerParam(rule, FROM, 10);
		assertTrue(rule.isReady());
		
		rule = new StringLengthValidationRule();
		setStringParam(rule, TARGET, "some text");
		setEnumParam(rule, TYPE, StringLengthValidationRule.TYPES.MINIMAL);
		assertFalse(rule.isReady());
		setIntegerParam(rule, TO, 20);
		assertFalse(rule.isReady());
		setIntegerParam(rule, FROM, 10);
		assertTrue(rule.isReady());
		
		rule = new StringLengthValidationRule();
		setStringParam(rule, TARGET, "some text");
		setEnumParam(rule, TYPE, StringLengthValidationRule.TYPES.MAXIMAL);
		assertFalse(rule.isReady());
		setIntegerParam(rule, FROM, 10);
		assertFalse(rule.isReady());
		setIntegerParam(rule, TO, 20);
		assertTrue(rule.isReady());
		
		rule = new StringLengthValidationRule();
		setStringParam(rule, TARGET, "some text");
		setEnumParam(rule, TYPE, StringLengthValidationRule.TYPES.INTERVAL);
		assertFalse(rule.isReady());
		setIntegerParam(rule, TO, 20);
		assertFalse(rule.isReady());
		setIntegerParam(rule, FROM, 10);
		assertTrue(rule.isReady());
	}
	@Test
	public void testNormal() {
		assertEquals(State.INVALID, createRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "1234"), null));
		assertEquals(State.VALID, createRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "12345"), null));
		assertEquals(State.INVALID, createRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "123456"), null));
		
		assertEquals(State.INVALID, createRule(">=", "field", 6, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "abcd"), null));
		assertEquals(State.VALID, createRule(">=", "field", 6, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdef"), null));
		assertEquals(State.VALID, createRule(">=", "field", 6, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdefgh"), null));
		
		assertEquals(State.VALID, createRule("<=", "field", 0, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcd"), null));
		assertEquals(State.VALID, createRule("<=", "field", 0, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdef"), null));
		assertEquals(State.INVALID, createRule("<=", "field", 0, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdefgh"), null));
		
		assertEquals(State.INVALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "ab"), null));
		assertEquals(State.VALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abc"), null));
		assertEquals(State.VALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcd"), null));
		assertEquals(State.VALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcde"), null));
		assertEquals(State.VALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdef"), null));
		assertEquals(State.INVALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdefgh"), null));
	}
	@Test
	public void testTrimming() {
		assertEquals(State.INVALID, createTrimmingRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", " 1234 "), null));
		assertEquals(State.VALID, createTrimmingRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "12345		"), null));
		assertEquals(State.INVALID, createTrimmingRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "\n123456\n"), null));
	}
	
	private StringLengthValidationRule createRule(String type, String target, int left, int right) {
		StringLengthValidationRule rule = new StringLengthValidationRule();
		rule.setEnabled(true);
		if(type.equals("==")) {
			setEnumParam(rule, TYPE, StringLengthValidationRule.TYPES.EXACT);
		} else if(type.equals(">=")) {
			setEnumParam(rule, TYPE, StringLengthValidationRule.TYPES.MINIMAL);
		} else if(type.equals("<=")) {
			setEnumParam(rule, TYPE, StringLengthValidationRule.TYPES.MAXIMAL);
		} else if(type.equals("<>")) {
			setEnumParam(rule, TYPE, StringLengthValidationRule.TYPES.INTERVAL);
		} else {
			fail("Cannot match operator to create rule.");
		}
		setStringParam(rule, TARGET, target);
		setIntegerParam(rule, FROM, left);
		setIntegerParam(rule, TO, right);
		return rule;
	}
	
	private StringLengthValidationRule createTrimmingRule(String type, String target, int left, int right) {
		StringLengthValidationRule rule = createRule(type, target, left, right);
		setBooleanParam(rule, TRIM, true);
		return rule;
	}
}
