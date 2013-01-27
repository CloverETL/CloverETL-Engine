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
import org.jetel.data.primitive.DecimalFactory;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.12.2012
 */
public class NonEmptyFieldValidationRuleTest extends ValidatorTestCase {
	
	@Test
	public void testNameability() {
		testNameability(NonEmptyFieldValidationRule.class);
	}
	
	@Test
	public void testDisablity() {
		testDisability(NonEmptyFieldValidationRule.class);
	}
	@Test
	public void testAttributes() {
		testBooleanAttribute(NonEmptyFieldValidationRule.class, NonEmptyFieldValidationRule.TRIM, false);
		testStringAttribute(NonEmptyFieldValidationRule.class, NonEmptyFieldValidationRule.TARGET, "");
		testBooleanAttribute(NonEmptyFieldValidationRule.class, NonEmptyFieldValidationRule.GOAL, false);
	}
	@Test
	public void testReadyness() {
		AbstractValidationRule rule = new NonEmptyFieldValidationRule();
		assertFalse(rule.isReady());
		setStringParam(rule, NonEmptyFieldValidationRule.TARGET, "some_text");
		assertTrue(rule.isReady());
		setStringParam(rule, NonEmptyFieldValidationRule.TARGET, "");
		assertFalse(rule.isReady());
	}

	@Test
	public void testEmptiness() {
		AbstractValidationRule rule = new NonEmptyFieldValidationRule();
		setStringParam(rule, NonEmptyFieldValidationRule.TARGET, "email");
		setBooleanParam(rule, NonEmptyFieldValidationRule.GOAL, true);
		rule.setEnabled(true);
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", ""), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", null), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", null), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", null), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", null), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", null), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", null), null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "some text"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "          "), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "	"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", 0l), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", 0), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", true), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", false), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", DecimalFactory.getDecimal(0)), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", 0d), null));
	}
	@Test
	public void testNonEmptiness() {
		AbstractValidationRule rule = new NonEmptyFieldValidationRule();
		setStringParam(rule, NonEmptyFieldValidationRule.TARGET, "email");
		rule.setEnabled(true);
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", ""), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", null), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", null), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", null), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", null), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", null), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", null), null));
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "some text"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "          "), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "	"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", 0l), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", 0), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", true), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", false), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", DecimalFactory.getDecimal(0)), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", 0d), null));
		
	}
	
	@Test
	public void testTrimming() {		
		AbstractValidationRule rule = new NonEmptyFieldValidationRule();
		setStringParam(rule, NonEmptyFieldValidationRule.TARGET, "email");
		setBooleanParam(rule, NonEmptyFieldValidationRule.TRIM, true);
		rule.setEnabled(true);
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "   "), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "	"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "\n"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "  some text  "), null));
		
		
		AbstractValidationRule rule2 = new NonEmptyFieldValidationRule();
		setStringParam(rule2, NonEmptyFieldValidationRule.TARGET, "email");
		setBooleanParam(rule2, NonEmptyFieldValidationRule.GOAL, true);
		setBooleanParam(rule2, NonEmptyFieldValidationRule.TRIM, true);
		rule2.setEnabled(true);
		assertEquals(State.VALID, rule2.isValid(TestDataRecordFactory.addStringField(null, "email", "   "), null));
		assertEquals(State.VALID, rule2.isValid(TestDataRecordFactory.addStringField(null, "email", "	"), null));
		assertEquals(State.VALID, rule2.isValid(TestDataRecordFactory.addStringField(null, "email", "\n"), null));
		assertEquals(State.INVALID, rule2.isValid(TestDataRecordFactory.addStringField(null, "email", "  some text  "), null));
	}
	

}
