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
	
//	private static final String TARGET = "target";
//	private static final String TRIM = "trimInput";
//	private static final String PATTERN = "pattern";
//	private static final String IGNORE_CASE = "ignoreCase";
	
//	@Test
//	public void testNameability() {
//		testNameability(PatternMatchValidationRule.class);
//	}
//	
//	@Test
//	public void testDisablity() {
//		testDisability(PatternMatchValidationRule.class);
//	}
//	@Test
//	public void testAttributes() {
//		testBooleanAttribute(PatternMatchValidationRule.class, IGNORE_CASE, false);
//		testBooleanAttribute(PatternMatchValidationRule.class, TRIM, false);
//		testStringAttribute(PatternMatchValidationRule.class, TARGET, "");
//		testStringAttribute(PatternMatchValidationRule.class, PATTERN, "");
//	}
//	@Test
//	public void testReadyness() {
//		AbstractValidationRule rule = new PatternMatchValidationRule();
//		assertFalse(rule.isReady());
//		setStringParam(rule, TARGET, "some_text");
//		assertFalse(rule.isReady());
//		setStringParam(rule, PATTERN , "");
//		assertFalse(rule.isReady());
//		setStringParam(rule, PATTERN , "pattern");
//		assertTrue(rule.isReady());
//		setStringParam(rule, TARGET, "");
//		assertFalse(rule.isReady());
//	}
	@Test
	public void testJavaSyntax() {
		DateValidationRule rule = new DateValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		
		// Default pattern (DateTime)
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-08"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-08 10:20:30"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-6-8 10:20:30"), null));
		// assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-13-08 10:20:30"), null));
		// assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2011-2-29 10:20:30"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-asdf"), null));
		
		// Own pattern
		rule.getFormat().setValue("yyyy-MM-DD");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-08"), null));
		//assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-600-08"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-cc"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-0a-32"), null));
		
		rule.getFormat().setValue("yyMMddHHmmssZ");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "010704120856-0700"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "010704120856"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "010704120856abc"), null));
		
		rule.getFormat().setValue("h:mm a");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "12:08 PM"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "12a:08 PM"), null));
		
		rule.getFormat().setValue("EEE, MMM d, ''yy");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Wed, Jul 4, '01"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SSS, Jul 4, '01"), null));
		
		rule.getFormat().setValue("yyyy.MM.dd G 'at' HH:mm:ss z");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2001.07.04 AD at 12:08:56 PDT"), null));
		
		rule.getFormat().setValue("hh 'o''clock' a, zzzz");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "12 o'clock PM, Pacific Daylight Time"), null));
	}
	@Test
	public void testJavaSyntaxStrictMode() {
		DateValidationRule rule = new DateValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		rule.getStrict().setValue(true);
		
		// Default pattern (DateTime)
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-08"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-08 10:20:30"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-6-8 10:20:30"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-13-08 10:20:30"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2011-2-29 10:20:30"), null));
		
		// Own pattern
		rule.getFormat().setValue("yyyy-MM-dd");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-08"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-13-08"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-600-08"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-06-cc"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-0a-32"), null));
	}
	
	@Test
	public void testJodaSyntax() {
		DateValidationRule rule = new DateValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		
		rule.getFormat().setValue("joda:yyyy-MM-dd");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-02-02"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2011-02-29"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012a-02-02"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-2-2"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-2-2a"), null));
	}
	
	@Test
	public void testJodaSyntaxStrictMode() {
		DateValidationRule rule = new DateValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue("field");
		
		rule.getFormat().setValue("joda:yyyy-MM-dd");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-02-02"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2011-02-29"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012a-02-02"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-2-2"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "2012-2-2a"), null));
	}
}
