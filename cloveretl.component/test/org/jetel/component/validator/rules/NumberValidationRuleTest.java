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
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.data.DataRecord;
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
	public void testCommon() {
		testCommon(NumberValidationRule.class);
	}

	@Test
	public void testReadyness() {
		DataRecord record = RF.addStringField(null, "field", null);
		DataRecord record2 = RF.addLongField(null, "field", null);
		DateValidationRule rule = createRule(DateValidationRule.class);
		
		assertReadyness(false, rule, record.getMetadata());	// No target
		
		rule.getTarget().setValue("field");
		assertReadyness(true, rule, record.getMetadata());
		assertReadyness(false, rule, record2.getMetadata()); // Not string target field
		
		rule.getTarget().setValue("field2");
		assertReadyness(false, rule, record.getMetadata());	// Non-existent target field
		
		rule.getTarget().setValue("field");
		
		rule.setParentLanguageSetting(defaultLanguageSetting());
		rule.getParentLanguageSetting().getDateFormat().setValue("");
		assertReadyness(false, rule, record.getMetadata());	// Empty number format
		
		rule.setParentLanguageSetting(defaultLanguageSetting());
		rule.getParentLanguageSetting().getLocale().setValue("");
		assertReadyness(false, rule, record.getMetadata());	// Empty locale
	}
	@Test
	public void testNullInput() {
		NumberValidationRule rule = createRule(NumberValidationRule.class);
		rule.getTarget().setValue("field");
		assertValid(rule, RF.addStringField(null, "field", null));
	}
	@Test
	public void testInteger() {
		NumberValidationRule rule = createRule(NumberValidationRule.class);
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		rule.getLanguageSettings(0).getNumberFormat().setValue(CommonFormats.INTEGER);
		
		assertInvalid(rule, RF.addStringField(null, "field", ""));
		
		assertValid(rule, RF.addStringField(null, "field", "10"));
		assertInvalid(rule, RF.addStringField(null, "field", "10.0"));
		assertInvalid(rule, RF.addStringField(null, "field", "11.35"));
		assertInvalid(rule, RF.addStringField(null, "field", "-20,5"));
		assertInvalid(rule, RF.addStringField(null, "field", "-20.5"));
		
		assertInvalid(rule, RF.addStringField(null, "field", "abc"));
		assertInvalid(rule, RF.addStringField(null, "field", "-20.5a"));
		assertInvalid(rule, RF.addStringField(null, "field", "-a20.5"));
		assertInvalid(rule, RF.addStringField(null, "field", "-"));
		assertValid(rule, RF.addStringField(null, "field", "0"));
		assertInvalid(rule, RF.addStringField(null, "field", "+"));
		
		assertValid(rule, RF.addStringField(null, "field", "10"));
		assertValid(rule, RF.addStringField(null, "field", "-1"));
		assertInvalid(rule, RF.addStringField(null, "field", "-"));
		assertInvalid(rule, RF.addStringField(null, "field", "+"));
		assertInvalid(rule, RF.addStringField(null, "field", "+588"));
		assertInvalid(rule, RF.addStringField(null, "field", "-12.5"));
		assertInvalid(rule, RF.addStringField(null, "field", "2.48"));
		assertInvalid(rule, RF.addStringField(null, "field", "1.980000"));
		assertInvalid(rule, RF.addStringField(null, "field", "10a"));
		assertInvalid(rule, RF.addStringField(null, "field", "-12.a5"));
		assertInvalid(rule, RF.addStringField(null, "field", "-20.5a"));
		assertInvalid(rule, RF.addStringField(null, "field", "-20,5"));
	}
	@Test
	public void testNumber() {
		NumberValidationRule rule = createRule(NumberValidationRule.class);
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		rule.getLanguageSettings(0).getNumberFormat().setValue(CommonFormats.NUMBER);
	
		assertInvalid(rule, RF.addStringField(null, "field", ""));
		
		assertValid(rule, RF.addStringField(null, "field", "10"));
		assertValid(rule, RF.addStringField(null, "field", "10.0"));
		assertValid(rule, RF.addStringField(null, "field", "11.35"));
		assertInvalid(rule, RF.addStringField(null, "field", "-20,5"));
		assertValid(rule, RF.addStringField(null, "field", "-20.5"));
		
		assertInvalid(rule, RF.addStringField(null, "field", "abc"));
		assertInvalid(rule, RF.addStringField(null, "field", "-20.5a"));
		assertInvalid(rule, RF.addStringField(null, "field", "-a20.5"));
		assertInvalid(rule, RF.addStringField(null, "field", "-"));
		assertValid(rule, RF.addStringField(null, "field", "0"));
		assertInvalid(rule, RF.addStringField(null, "field", "+"));
		
		assertValid(rule, RF.addStringField(null, "field", "10"));
		assertValid(rule, RF.addStringField(null, "field", "-1"));
		assertInvalid(rule, RF.addStringField(null, "field", "-"));
		assertInvalid(rule, RF.addStringField(null, "field", "+"));
		assertInvalid(rule, RF.addStringField(null, "field", "+588"));
		assertValid(rule, RF.addStringField(null, "field", "-12.5"));
		assertValid(rule, RF.addStringField(null, "field", "2.48"));
		assertValid(rule, RF.addStringField(null, "field", "1.980000"));
		assertInvalid(rule, RF.addStringField(null, "field", "10a"));
		assertInvalid(rule, RF.addStringField(null, "field", "-12.a5"));
		assertInvalid(rule, RF.addStringField(null, "field", "-20.5a"));
		assertInvalid(rule, RF.addStringField(null, "field", "-20,5"));
	}

	public void testCustomFormats() {
		NumberValidationRule rule = createRule(NumberValidationRule.class);
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		
		rule.getLanguageSettings(0).getLocale().setValue("en.US");
		rule.getLanguageSettings(0).getNumberFormat().setValue("#0.#####E0");	
		assertValid(rule, RF.addStringField(null, "field", "1.2345E4"));
		assertInvalid(rule, RF.addStringField(null, "field", "1.2345EA"));
		assertValid(rule, RF.addStringField(null, "field", "122"));
		assertValid(rule, RF.addStringField(null, "field", "1"));
		assertInvalid(rule, RF.addStringField(null, "field", ""));
		
		rule.getLanguageSettings(0).getNumberFormat().setValue("INTEGER");
		assertValid(rule, RF.addStringField(null, "field", "1"));
		assertValid(rule, RF.addStringField(null, "field", "10"));
		assertValid(rule, RF.addStringField(null, "field", "-5"));
		assertValid(rule, RF.addStringField(null, "field", "-25"));
		assertInvalid(rule, RF.addStringField(null, "field", "1.1"));
		assertInvalid(rule, RF.addStringField(null, "field", "10,5"));
		assertInvalid(rule, RF.addStringField(null, "field", "-5.8"));
		assertInvalid(rule, RF.addStringField(null, "field", "-25.9870"));
		
		rule.getLanguageSettings(0).getNumberFormat().setValue("###,###.###");
		assertValid(rule, RF.addStringField(null, "field", "1"));
		assertValid(rule, RF.addStringField(null, "field", "1234567890"));	// WTF? Java parsing :-D
		assertValid(rule, RF.addStringField(null, "field", "1,234,567.890"));
		
		rule.getLanguageSettings(0).getNumberFormat().setValue("#");
		assertInvalid(rule, RF.addStringField(null, "field", "123,456.890"));
	}
	
	public void testLocale() {
		NumberValidationRule rule = createRule(NumberValidationRule.class);
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		
		rule.getLanguageSettings(0).getLocale().setValue("cs.CZ");
		assertValid(rule, RF.addStringField(null, "field", "1,23454"));
		assertInvalid(rule, RF.addStringField(null, "field", "1.23454"));
		assertInvalid(rule, RF.addStringField(null, "field", "1.2345EA"));
		assertValid(rule, RF.addStringField(null, "field", "122"));
		assertValid(rule, RF.addStringField(null, "field", "1"));
		assertInvalid(rule, RF.addStringField(null, "field", ""));
		
		rule.getLanguageSettings(0).getLocale().setValue("fr");
		assertValid(rule, RF.addStringField(null, "field", "1,23454"));
		assertInvalid(rule, RF.addStringField(null, "field", "1.23454"));
		assertInvalid(rule, RF.addStringField(null, "field", "1.2345EA"));
	}
}
