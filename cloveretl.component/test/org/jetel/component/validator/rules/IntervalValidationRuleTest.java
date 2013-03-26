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

import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.common.TestDataRecordFactory;
import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 9.1.2013
 */
public class IntervalValidationRuleTest extends ValidatorTestCase {
	
	@Test
	public void testNameability() {
		testNameability(IntervalValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(IntervalValidationRule.class);
	}
//	@Test
//	public void testAttributes() {
//		testStringAttribute(RangeCheckValidationRule.class, TARGET, "");
//		testEnumAttribute(RangeCheckValidationRule.class, TYPE, RangeCheckValidationRule.TYPES.values(), RangeCheckValidationRule.TYPES.COMPARISON);
//		
//		testEnumAttribute(RangeCheckValidationRule.class, OPERATOR, RangeCheckValidationRule.OPERATOR_TYPE.values(), RangeCheckValidationRule.OPERATOR_TYPE.E);
//		testStringAttribute(RangeCheckValidationRule.class, VALUE, "");
//		
//		testEnumAttribute(RangeCheckValidationRule.class, BOUNDARIES, RangeCheckValidationRule.BOUNDARIES_TYPE.values(), RangeCheckValidationRule.BOUNDARIES_TYPE.CLOSED_CLOSED);
//		testStringAttribute(RangeCheckValidationRule.class, FROM, "");
//		testStringAttribute(RangeCheckValidationRule.class, TO, "");
//		
//		testEnumAttribute(RangeCheckValidationRule.class, USE_TYPE, RangeCheckValidationRule.METADATA_TYPES.values(), RangeCheckValidationRule.METADATA_TYPES.DEFAULT);		
//	}
//	@Test
//	public void testReadynessComparison() {
//		RangeCheckValidationRule rule = new RangeCheckValidationRule();
//		assertFalse(rule.isReady());
//		
//		// TBD: tests
//	}
//	@Test
//	public void testReadynessInterval() {
//		RangeCheckValidationRule rule = new RangeCheckValidationRule();
//		assertFalse(rule.isReady());
//		
//		// TBD: tests
//	}
	
	@Test
	public void testStringInStringInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "ce"), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "cep"), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "noha"), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "opera"), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "operaa"), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "ce"), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "cep"), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "noha"), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "opera"), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "operaa"), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "ce"), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "cep"), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "noha"), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "opera"), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "operaa"), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "ce"), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "cep"), null, null));
		assertEquals(State.VALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "noha"), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "opera"), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "operaa"), null, null));
	}
	@Test
	public void testLongInLongInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 4l), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 5l), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 8l), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 9l), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 4l), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 5l), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 8l), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 9l), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 4l), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 5l), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 8l), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 9l), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 4l), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 5l), null, null));
		assertEquals(State.VALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 8l), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 9l), null, null));
	}
	@Test
	public void testIntegerInIntegerInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 4), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 5), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 8), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 9), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 4), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 5), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 8), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 9), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 4), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 5), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 8), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 9), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 4), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 5), null, null));
		assertEquals(State.VALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 8), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 9), null, null));
	}
	@Test
	public void testNumberInNumberInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5d), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5.66d), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.85d), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.9d), null, null));

		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5d), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5.66d), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.85d), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.9d), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5d), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5.66d), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.85d), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.9d), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5d), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5.66d), null, null));
		assertEquals(State.VALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.85d), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.9d), null, null));
	}
	@Test
	public void testDecimalInDecimalInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5")), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null, null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.85")), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.9")), null, null));

		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5")), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null, null));
		assertEquals(State.VALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.85")), null, null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.9")), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5")), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null, null));
		assertEquals(State.VALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.85")), null, null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.9")), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5")), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null, null));
		assertEquals(State.VALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.85")), null, null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.9")), null, null));
	}
	
	@Test
	public void testInvalidInputs() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66A", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66A", "7,85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66a", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.dd85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null, null));
		
		assertEquals(State.INVALID, createInterval("field", "[]", "a5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8,0").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8.0").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null, null));
	}
	@Test
	public void testNonConvertible() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "a", "s").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "a", "s").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7")), null, null));
		assertEquals(State.INVALID, createInterval("field", "[]", "a", "s").isValid(TestDataRecordFactory.addNumberField(null, "field", 7d), null, null));
	}
	
	public void testUserDataType() {
		// As strings
		assertEquals(State.VALID, inType("s",createInterval("field", "[]", "5.66", "7.85")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null, null));
		assertEquals(State.INVALID, inType("s",createInterval("field", "[]", "5.66", "100.85")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("10.66")), null, null));		
	}
	
	public void testDates() {
		assertEquals(State.VALID, inType("da",createInterval("field", "[]", "2012-01-06 00:00:00", "2012-01-28 00:00:00","yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field", "2012-01-17"), null, null));
		assertEquals(State.VALID, inType("da",createInterval("field", "[]", "2012-01-06 00:00:00", "2012-01-28 00:00:00","yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field", "2012-01-28"), null, null));
		assertEquals(State.VALID, inType("da",createInterval("field", "[]", "2012-01-06 00:00:00", "2012-01-28 00:00:00","yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field", "2012-01-06"), null, null));
		assertEquals(State.INVALID, inType("da",createInterval("field", "[]", "2012-01-06 00:00:00", "2012-01-28 00:00:00","yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field", "2012-0100-06"), null, null));
		assertEquals(State.INVALID, inType("da",createInterval("field", "[]", "2012-01-06 00:00:00", "2012-01-28 00:00:00","yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field", "2012-01a-17"), null, null));
		assertEquals(State.INVALID, inType("da",createInterval("field", "[]", "2012-01-06 00:00:00", "2012-01-28 00:00:00","yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field", "2012-01-05"), null, null));
		assertEquals(State.INVALID, inType("da",createInterval("field", "[]", "2012-01-06 00:00:00", "2012-01-28 00:00:00","yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field", "2012-01-17d"), null, null));
		assertEquals(State.INVALID, inType("da",createInterval("field", "[]", "2012-01-06 00:00:00", "2012-01-28 00:00:00","yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field", "2012-01-28dd"), null, null));
		assertEquals(State.INVALID, inType("da",createInterval("field", "[]", "2012-01-06 00:00:00", "2012-01-28 00:00:00","yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field", "2012-01-06asd"), null, null));
	}
	
	private IntervalValidationRule createInterval(String target, String interval, String from, String to) {
		IntervalValidationRule.BOUNDARIES_TYPE b = null;
		if(interval.equals("[]")) {
			b = IntervalValidationRule.BOUNDARIES_TYPE.CLOSED_CLOSED;
		} else if(interval.equals("[)")) {
			b = IntervalValidationRule.BOUNDARIES_TYPE.CLOSED_OPEN;
		} else if(interval.equals("(]")) {
			b = IntervalValidationRule.BOUNDARIES_TYPE.OPEN_CLOSED;
		} else if(interval.equals("()")) {
			b = IntervalValidationRule.BOUNDARIES_TYPE.OPEN_OPEN;
		}
		IntervalValidationRule rule = new IntervalValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue(target);
		rule.getBoundaries().setValue(b);
		rule.getFrom().setValue(from);
		rule.getTo().setValue(to);
		return rule;
	}
	private IntervalValidationRule createInterval(String target, String operator, String from, String to, String format, boolean strict) {
		IntervalValidationRule rule = createInterval(target, operator, from, to);
		rule.getFormat().setValue(format);
		return rule;
	}
	
	private IntervalValidationRule inType(String type, IntervalValidationRule rule) {
		IntervalValidationRule.METADATA_TYPES t = null;
		if(type.equals("s")) {
			t = ConversionValidationRule.METADATA_TYPES.STRING;
		} else if(type.equals("l")) {
			t = ConversionValidationRule.METADATA_TYPES.LONG;
		} else if(type.equals("da")) {
			t = ConversionValidationRule.METADATA_TYPES.DATE;
		} else if(type.equals("n")) {
			t = ConversionValidationRule.METADATA_TYPES.NUMBER;
		} else if(type.equals("d")) {
			t = ConversionValidationRule.METADATA_TYPES.DECIMAL;
		}
		rule.getUseType().setValue(t);
		return rule;
	}
}
