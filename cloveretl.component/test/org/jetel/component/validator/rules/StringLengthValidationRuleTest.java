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

	@Test
	public void testNameability() {
		testNameability(StringLengthValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(StringLengthValidationRule.class);
	}
	@Test
	public void testReadyness() {
		// TODO
	}
	@Test
	public void testNormal() {
		assertEquals(State.INVALID, createRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "1234"), null, null));
		assertEquals(State.VALID, createRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "12345"), null, null));
		assertEquals(State.INVALID, createRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "123456"), null, null));
		
		assertEquals(State.INVALID, createRule(">=", "field", 6, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "abcd"), null, null));
		assertEquals(State.VALID, createRule(">=", "field", 6, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdef"), null, null));
		assertEquals(State.VALID, createRule(">=", "field", 6, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdefgh"), null, null));
		
		assertEquals(State.VALID, createRule("<=", "field", 0, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcd"), null, null));
		assertEquals(State.VALID, createRule("<=", "field", 0, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdef"), null, null));
		assertEquals(State.INVALID, createRule("<=", "field", 0, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdefgh"), null, null));
		
		assertEquals(State.INVALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "ab"), null, null));
		assertEquals(State.VALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abc"), null, null));
		assertEquals(State.VALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcd"), null, null));
		assertEquals(State.VALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcde"), null, null));
		assertEquals(State.VALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdef"), null, null));
		assertEquals(State.INVALID, createRule("<>", "field", 3, 6).isValid(TestDataRecordFactory.addStringField(null, "field", "abcdefgh"), null, null));
	}
	@Test
	public void testTrimming() {
		assertEquals(State.INVALID, createTrimmingRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", " 1234 "), null, null));
		assertEquals(State.VALID, createTrimmingRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "12345		"), null, null));
		assertEquals(State.INVALID, createTrimmingRule("==", "field", 5, 0).isValid(TestDataRecordFactory.addStringField(null, "field", "\n123456\n"), null, null));
	}
	
	private StringLengthValidationRule createRule(String type, String target, int left, int right) {
		StringLengthValidationRule rule = new StringLengthValidationRule();
		rule.setEnabled(true);
		if(type.equals("==")) {
			rule.getType().setValue(StringLengthValidationRule.TYPES.EXACT);
		} else if(type.equals(">=")) {
			rule.getType().setValue(StringLengthValidationRule.TYPES.MINIMAL);
		} else if(type.equals("<=")) {
			rule.getType().setValue(StringLengthValidationRule.TYPES.MAXIMAL);
		} else if(type.equals("<>")) {
			rule.getType().setValue(StringLengthValidationRule.TYPES.INTERVAL);
		} else {
			fail("Cannot match operator to create rule.");
		}
		rule.getFrom().setValue(left);
		rule.getTo().setValue(right);
		rule.getTarget().setValue(target);
		return rule;
	}
	
	private StringLengthValidationRule createTrimmingRule(String type, String target, int left, int right) {
		StringLengthValidationRule rule = createRule(type, target, left, right);
		rule.getTrimInput().setValue(true);
		return rule;
	}
}
