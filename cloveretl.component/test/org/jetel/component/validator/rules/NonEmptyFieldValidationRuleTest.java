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

import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.data.DataRecord;
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
	public void testCommon() {
		testCommon(NonEmptyFieldValidationRule.class);
	}
	@Test
	public void testReadyness() {
		NonEmptyFieldValidationRule rule = createRule(NonEmptyFieldValidationRule.class);
		DataRecord record = RF.addStringField(null, "email2", "");
		
		assertReadyness(false, rule, record.getMetadata());
		rule.getTarget().setValue("email");
		assertReadyness(false, rule, record.getMetadata());
		rule.getTarget().setValue("email2");
		assertReadyness(true, rule, record.getMetadata());
	}

	@Test
	public void testEmptiness() {
		NonEmptyFieldValidationRule rule = createRule(NonEmptyFieldValidationRule.class);
		rule.getTarget().setValue("email");
		rule.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.EMPTY);
		
		assertValid(rule,RF.addStringField(null, "email", ""));
		assertValid(rule,RF.addStringField(null, "email", ""));
		assertValid(rule,RF.addStringField(null, "email", null));
		assertValid(rule,RF.addLongField(null, "email", null));
		assertValid(rule,RF.addIntegerField(null, "email", null));
		assertValid(rule,RF.addBooleanField(null, "email", null));
		assertValid(rule,RF.addDecimalField(null, "email", null));
		assertValid(rule,RF.addNumberField(null, "email", null));
		
		assertInvalid(rule, RF.addStringField(null, "email", "some text"));
		assertInvalid(rule, RF.addStringField(null, "email", "          "));
		assertInvalid(rule, RF.addStringField(null, "email", "	"));
		assertInvalid(rule, RF.addLongField(null, "email", 0l));
		assertInvalid(rule, RF.addIntegerField(null, "email", 0));
		assertInvalid(rule, RF.addBooleanField(null, "email", true));
		assertInvalid(rule, RF.addBooleanField(null, "email", false));
		assertInvalid(rule, RF.addDecimalField(null, "email", DecimalFactory.getDecimal(0)));
		assertInvalid(rule, RF.addNumberField(null, "email", 0d));
	}
	@Test
	public void testNonEmptiness() {
		NonEmptyFieldValidationRule rule = createRule(NonEmptyFieldValidationRule.class);
		rule.getTarget().setValue("email");
		rule.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.NONEMPTY);
		assertInvalid(rule, RF.addStringField(null, "email", ""));
		assertInvalid(rule, RF.addStringField(null, "email", null));
		assertInvalid(rule, RF.addLongField(null, "email", null));
		assertInvalid(rule, RF.addIntegerField(null, "email", null));
		assertInvalid(rule, RF.addBooleanField(null, "email", null));
		assertInvalid(rule, RF.addDecimalField(null, "email", null));
		assertInvalid(rule, RF.addNumberField(null, "email", null));
		
		assertValid(rule,RF.addStringField(null, "email", "some text"));
		assertValid(rule,RF.addStringField(null, "email", "          "));
		assertValid(rule,RF.addStringField(null, "email", "	"));
		assertValid(rule,RF.addLongField(null, "email", 0l));
		assertValid(rule,RF.addIntegerField(null, "email", 0));
		assertValid(rule,RF.addBooleanField(null, "email", true));
		assertValid(rule,RF.addBooleanField(null, "email", false));
		assertValid(rule,RF.addDecimalField(null, "email", DecimalFactory.getDecimal(0)));
		assertValid(rule,RF.addNumberField(null, "email", 0d));
		
	}
	
	@Test
	public void testTrimming() {		
		NonEmptyFieldValidationRule rule = createRule(NonEmptyFieldValidationRule.class);
		rule.getTarget().setValue("email");
		rule.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.NONEMPTY);
		rule.getTrimInput().setValue(true);
		assertInvalid(rule, RF.addStringField(null, "email", "   "));
		assertInvalid(rule, RF.addStringField(null, "email", "	"));
		assertInvalid(rule, RF.addStringField(null, "email", "\n"));
		assertValid(rule,RF.addStringField(null, "email", "  some text  "));
		
		
		NonEmptyFieldValidationRule rule2 = createRule(NonEmptyFieldValidationRule.class);
		rule2.getTarget().setValue("email");
		rule2.getTrimInput().setValue(true);
		rule2.getGoal().setValue(NonEmptyFieldValidationRule.GOALS.EMPTY);
		assertValid(rule2,RF.addStringField(null, "email", "   "));
		assertValid(rule2,RF.addStringField(null, "email", "	"));
		assertValid(rule2,RF.addStringField(null, "email", "\n"));
		assertInvalid(rule2, RF.addStringField(null, "email", "  some text  "));
	}
}
