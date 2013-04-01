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
import org.jetel.component.validator.ValidationError;
import org.jetel.component.validator.ValidationErrorAccumulator;
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
	public void testReadyness() {
		// TODO:
	}

	@Test
	public void testEmptiness() {
		NonEmptyFieldValidationRule rule = new NonEmptyFieldValidationRule();
		rule.getTarget().setValue("email");
		rule.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.EMPTY);
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", ""), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", null), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", null), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", null), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", null), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", null), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", null), null, null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "some text"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "          "), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "	"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", 0l), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", 0), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", true), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", false), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", DecimalFactory.getDecimal(0)), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", 0d), null, null));
	}
	@Test
	public void testNonEmptiness() {
		NonEmptyFieldValidationRule rule = new NonEmptyFieldValidationRule();
		rule.getTarget().setValue("email");
		rule.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.NONEMPTY);
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", ""), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", null), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", null), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", null), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", null), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", null), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", null), null, null));
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "some text"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "          "), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "	"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", 0l), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", 0), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", true), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", false), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", DecimalFactory.getDecimal(0)), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", 0d), null, null));
		
	}
	
	@Test
	public void testTrimming() {		
		NonEmptyFieldValidationRule rule = new NonEmptyFieldValidationRule();
		rule.getTarget().setValue("email");
		rule.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.NONEMPTY);
		rule.getTrimInput().setValue(true);
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "   "), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "	"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "\n"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "  some text  "), null, null));
		
		
		NonEmptyFieldValidationRule rule2 = new NonEmptyFieldValidationRule();
		rule2.getTarget().setValue("email");
		rule2.getTrimInput().setValue(true);
		rule2.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.EMPTY);
		assertEquals(State.VALID, rule2.isValid(TestDataRecordFactory.addStringField(null, "email", "   "), null, null));
		assertEquals(State.VALID, rule2.isValid(TestDataRecordFactory.addStringField(null, "email", "	"), null, null));
		assertEquals(State.VALID, rule2.isValid(TestDataRecordFactory.addStringField(null, "email", "\n"), null, null));
		assertEquals(State.INVALID, rule2.isValid(TestDataRecordFactory.addStringField(null, "email", "  some text  "), null, null));
	}
	
	public void testNonStringType() {
		NonEmptyFieldValidationRule rule = new NonEmptyFieldValidationRule();
		rule.getTarget().setValue("email");
		rule.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.NONEMPTY);
		rule.getTrimInput().setValue(true);
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", null), null, null));
		
		rule.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.EMPTY);
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", null), null, null));
	}
	
	public void testErrorAccumulator() {
		NonEmptyFieldValidationRule rule = new NonEmptyFieldValidationRule();
		rule.getTarget().setValue("email");
		rule.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.EMPTY);
		ValidationErrorAccumulator accumulator = new ValidationErrorAccumulator();
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", ""), accumulator, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", null), accumulator, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", null), accumulator, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", null), accumulator, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", null), accumulator, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", null), accumulator, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", null), accumulator, null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "some text"), accumulator, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "          "), accumulator, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "email", "	"), accumulator, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addLongField(null, "email", 0l), accumulator, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "email", 0), accumulator, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", true), accumulator, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "email", false), accumulator, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "email", DecimalFactory.getDecimal(0)), accumulator, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "email", 0d), accumulator, null));
		
		
		for(ValidationError error : accumulator) {
			System.err.println(error.toString());
		}
	}
	

}
