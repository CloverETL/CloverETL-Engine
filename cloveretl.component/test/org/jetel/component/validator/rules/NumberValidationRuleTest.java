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
 * @created 26.1.2013
 */
public class NumberValidationRuleTest extends ValidatorTestCase {
	
	@Test
	public void testNameability() {
		testNameability(NumberValidationRule.class);
	}
	
	@Test
	public void testDisability() {
		testDisability(NumberValidationRule.class);
	}

	@Test
	public void testReadyness() {
		// TODO
	}
	@Test
	public void testNullInput() {
		DateValidationRule rule = new DateValidationRule();
		rule.getTarget().setValue("field");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", null), null, null));
	}
	@Test
	public void testInteger() {
		NumberValidationRule rule = new NumberValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "", "en.US"), null, null));
		
		// 
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10.0", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "11.35", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20,5", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20.5", "en.US"), null, null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "abc", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20.5a", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-a20.5", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "0", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "+", "en.US"), null, null));
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-1", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "+", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "+588", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-12.5", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2.48", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1.980000", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10a", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-12.a5", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20.5a", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20,5", "en.US"), null, null));
	}
	@Test
	public void testNumber() {
		NumberValidationRule rule = new NumberValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		rule.getLanguageSettings(0).getNumberFormat().setValue("#");
	
		rule.getLanguageSettings(0).getLocale().setValue("en.US");
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "", "en.US"), null, null));
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10.0", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "11.35", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20,5", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20.5", "en.US"), null, null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "abc", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20.5a", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-a20.5", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "0", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "+", "en.US"), null, null));
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-1", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "+", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "+588", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-12.5", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2.48", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1.980000", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10a", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-12.a5", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20.5a", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-20,5", "en.US"), null, null));
	}

	public void testCustomFormats() {
		NumberValidationRule rule = new NumberValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		
		rule.getLanguageSettings(0).getLocale().setValue("en.US");
		rule.getLanguageSettings(0).getNumberFormat().setValue("#0.#####E0");	
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1.2345E4", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1.2345EA", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "122", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "", "en.US"), null, null));
		
		rule.getLanguageSettings(0).getNumberFormat().setValue("INTEGER");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-5", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-25", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1.1", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10,5", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-5.8", "en.US"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-25.9870", "en.US"), null, null));
		
		rule.getLanguageSettings(0).getNumberFormat().setValue("###,###.###");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1", "en.US"), null, null));
		// WTF?
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1234567890", "en.US"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1,234,567.890", "en.US"), null, null));
		
		rule.getLanguageSettings(0).getNumberFormat().setValue("#");
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "123,456.890", "en.US"), null, null));
	}
	
	public void testLocale() {
		NumberValidationRule rule = new NumberValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		
		rule.getLanguageSettings(0).getNumberFormat().setValue("#");
		rule.getLanguageSettings(0).getLocale().setValue("cs.CZ");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1,23454"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1.23454"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1.2345EA"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "122"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null, null));
		
		rule.getLanguageSettings(0).getLocale().setValue("fr");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1,23454"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1.23454"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1.2345EA"), null, null));
	}
}
