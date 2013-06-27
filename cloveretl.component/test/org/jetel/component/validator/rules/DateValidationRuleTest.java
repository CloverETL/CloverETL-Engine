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
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 26.1.2013
 */
public class DateValidationRuleTest extends ValidatorTestCase {
	
	@Test
	public void testNameability() {
		testNameability(DateValidationRule.class);
	}
	
	@Test
	public void testDisability() {
		testDisability(DateValidationRule.class);
	}
	@Test
	public void testCommon() {
		testCommon(DateValidationRule.class);
	}
	@Test
	public void testReadyness() {
		DataRecord record = RF.addStringField(null, "field", null);
		DataRecord record2 = RF.addLongField(null, "field", null);
		DateValidationRule rule = createRule(DateValidationRule.class);
		
		assertReadyness(false, rule, record.getMetadata());
		
		rule.getTarget().setValue("field");
		assertReadyness(true, rule, record.getMetadata());
		assertReadyness(false, rule, record2.getMetadata());
		
		rule.getTarget().setValue("field2");
		assertReadyness(false, rule, record.getMetadata());
		
		rule.getTarget().setValue("field");
		rule.getLanguageSettings(0).getDateFormat().setValue("Some wrong string");
		assertReadyness(false, rule, record.getMetadata());
		
		rule.setParentLanguageSetting(defaultLanguageSetting());
		rule.getParentLanguageSetting().getDateFormat().setValue("");
		assertReadyness(false, rule, record.getMetadata());
		
		rule.setParentLanguageSetting(defaultLanguageSetting());
		rule.getParentLanguageSetting().getLocale().setValue("");
		assertReadyness(false, rule, record.getMetadata());
		
		rule.setParentLanguageSetting(defaultLanguageSetting());
		rule.getParentLanguageSetting().getTimezone().setValue("");
		assertReadyness(false, rule, record.getMetadata());
		
	}
	@Test
	public void testNullInput() {
		DateValidationRule rule = createRule(DateValidationRule.class);
		rule.getTarget().setValue("field");
		assertValid(rule, RF.addStringField(null, "field", null));
	}
	@Test
	public void testJavaSyntax() {
		DateValidationRule rule = createRule(DateValidationRule.class);
		rule.getTarget().setValue("field");
		
		// Default pattern (DateTime)
		assertInvalid(rule, RF.addStringField(null, "field", "2012-06-08"));
		assertValid(rule, RF.addStringField(null, "field", "2012-06-08 10:20:30"));
		assertValid(rule, RF.addStringField(null, "field", "2012-6-8 10:20:30"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012-13-08 10:20:30"));
		assertInvalid(rule, RF.addStringField(null, "field", "2011-2-29 10:20:30"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012-asdf"));
		
		// Own pattern
		rule.getLanguageSettings(0).getDateFormat().setValue("yyyy-MM-dd");
		assertValid(rule, RF.addStringField(null, "field", "2012-06-08"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012-600-08"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012-06-cc"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012-0a-32"));
		
		rule.getLanguageSettings(0).getDateFormat().setValue("MM");
		assertValid(rule, RF.addStringField(null, "field", "01"));
		assertValid(rule, RF.addStringField(null, "field", "12"));
		assertInvalid(rule, RF.addStringField(null, "field", "13"));
		
		rule.getLanguageSettings(0).getDateFormat().setValue("yyMMddHHmmssZ");
		rule.getLanguageSettings(0).getTimezone().setValue("America/Los_Angeles");
		assertValid(rule, RF.addStringField(null, "field", "010704120856-0700"));
		assertInvalid(rule, RF.addStringField(null, "field", "010704120856"));
		assertInvalid(rule, RF.addStringField(null, "field", "010704120856abc"));
		
		rule.getLanguageSettings(0).getLocale().setValue("en.US");
		rule.getLanguageSettings(0).getDateFormat().setValue("h:mm a");
		assertValid(rule, RF.addStringField(null, "field", "12:08 PM"));
		assertInvalid(rule, RF.addStringField(null, "field", "12a:08 PM"));
		
		rule.getLanguageSettings(0).getDateFormat().setValue("EEE, MMM d, ''yy");
		assertValid(rule, RF.addStringField(null, "field", "Wed, Jul 4, '01"));
		assertInvalid(rule, RF.addStringField(null, "field", "SSS, Jul 4, '01"));
		
		rule.getLanguageSettings(0).getDateFormat().setValue("yyyy.MM.dd G 'at' HH:mm:ss z");
		assertValid(rule, RF.addStringField(null, "field", "2001.07.04 AD at 12:08:56 PDT"));
		
		// Will parse completely wrong
		//rule.getFormat().setValue("hh 'o''clock' a, zzzz");
		//assertValid(rule, RF.addStringField(null, "field", "12 o'clock PM, Pacific Daylight Time"), null));
	}
	
	@Test
	public void testJodaSyntax() {
		DateValidationRule rule = createRule(DateValidationRule.class);
		rule.getTarget().setValue("field");
		
		rule.getLanguageSettings(0).getDateFormat().setValue("joda:yyyy-MM-dd");
		assertValid(rule, RF.addStringField(null, "field", "2012-02-02"));
		assertInvalid(rule, RF.addStringField(null, "field", "2011-02-29"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012a-02-02"));
		assertValid(rule, RF.addStringField(null, "field", "2012-2-2"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012-2-2a"));
	}
	
	public void testLocale() {
		DateValidationRule rule = createRule(DateValidationRule.class);
		rule.getTarget().setValue("field");
		
		rule.getLanguageSettings(0).getLocale().setValue("hi.IN");
		assertValid(rule, RF.addStringField(null, "field", "२०११-०३-०३ १६:५३:०९"));
		assertValid(rule, RF.addStringField(null, "field", "2011-01-01 10:30:40"));
	}
	
	public void testMonthNames() {
		DateValidationRule rule = createRule(DateValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getLanguageSettings(0).getDateFormat().setValue("yyyy MMM d");
		rule.getLanguageSettings(0).getLocale().setValue("cs.CZ");
		
		assertValid(rule, RF.addStringField(null, "field", "2012 Prosinec 1"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012 December 1"));
		
		rule.getLanguageSettings(0).getLocale().setValue("en.EN");
		assertInvalid(rule, RF.addStringField(null, "field", "2012 Prosinec 1"));
		assertValid(rule, RF.addStringField(null, "field", "2012 December 1"));
	}
	
	@Test
	public void testNonStringInput() {
		DateValidationRule rule = createRule(DateValidationRule.class);
		rule.getTarget().setValue("field");
		
		assertFailsInit(rule, RF.addLongField(null, "field", 1000000l));
		assertFailsInit(rule, RF.addLongField(null, "field", 0l));
		assertFailsInit(rule, RF.addNumberField(null, "field", 1123.54));
		assertFailsInit(rule, RF.addNumberField(null, "field", 0d));
		assertFailsInit(rule, RF.addDecimalField(null, "field", getDecimal("12366.45")));
		assertFailsInit(rule, RF.addDecimalField(null, "field", getDecimal("0")));
		assertFailsInit(rule, RF.addBooleanField(null, "field", true));
		assertFailsInit(rule, RF.addBooleanField(null, "field", false));
	}
	
}
