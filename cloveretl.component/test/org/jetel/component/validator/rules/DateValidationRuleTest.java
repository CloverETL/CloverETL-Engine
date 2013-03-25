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
	public void testJavaSyntax() {
		DateValidationRule rule = new DateValidationRule();
		rule.getTarget().setValue("field");
		
		// Default pattern (DateTime)
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-08"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-08 10:20:30"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-6-8 10:20:30"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-13-08 10:20:30"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2011-2-29 10:20:30"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-asdf"), null, null));
		
		// Own pattern
		rule.getFormat().setValue("yyyy-MM-dd");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-08"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-600-08"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-cc"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-0a-32"), null, null));
		
		rule.getFormat().setValue("MM");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "01"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "12"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "13"), null, null));
		
		rule.getFormat().setValue("yyMMddHHmmssZ");
		rule.getTimezone().setValue("America/Los_Angeles");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "010704120856-0700"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "010704120856"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "010704120856abc"), null, null));
		
		rule.getLocale().setValue("en.US");
		rule.getFormat().setValue("h:mm a");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "12:08 PM"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "12a:08 PM"), null, null));
		
		rule.getFormat().setValue("EEE, MMM d, ''yy");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Wed, Jul 4, '01"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SSS, Jul 4, '01"), null, null));
		
		rule.getFormat().setValue("yyyy.MM.dd G 'at' HH:mm:ss z");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2001.07.04 AD at 12:08:56 PDT"), null, null));
		
		// Will parse completely wrong
		//rule.getFormat().setValue("hh 'o''clock' a, zzzz");
		//assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "12 o'clock PM, Pacific Daylight Time"), null));
	}
	
	@Test
	public void testJodaSyntax() {
		DateValidationRule rule = new DateValidationRule();
		rule.getTarget().setValue("field");
		
		rule.getFormat().setValue("joda:yyyy-MM-dd");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-02-02"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2011-02-29"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012a-02-02"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-2-2"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-2-2a"), null, null));
	}
	
	public void testLocale() {
		DateValidationRule rule = new DateValidationRule();
		rule.getTarget().setValue("field");
		
		rule.getLocale().setValue("hi.IN");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "२०११-०३-०३ १६:५३:०९"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2011-01-01 10:30:40"), null, null));
	}
	
	@Test
	public void testNonStringInput() {
		// TODO
	}
	
}
