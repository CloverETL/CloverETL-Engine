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
public class ComparisonValidationRuleTest extends ValidatorTestCase {
	
//	@Test
//	public void testNameability() {
//		testNameability(RangeCheckValidationRule.class);
//	}
//	@Test
//	public void testDisability() {
//		testDisability(RangeCheckValidationRule.class);
//	}
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
	public void testStringInStringComparison() {
		assertEquals(State.VALID, createComparison("number", "==", "b").isValid(TestDataRecordFactory.addStringField(null, "number","b"), null));
		assertEquals(State.INVALID, createComparison("number", "==", "b").isValid(TestDataRecordFactory.addStringField(null, "number","c"), null));
		assertEquals(State.INVALID, createComparison("number", "==", "b").isValid(TestDataRecordFactory.addStringField(null, "number","a"), null));
		
		assertEquals(State.INVALID, createComparison("number", "!=", "b").isValid(TestDataRecordFactory.addStringField(null, "number","b"), null));
		assertEquals(State.VALID, createComparison("number", "!=", "b").isValid(TestDataRecordFactory.addStringField(null, "number","c"), null));
		assertEquals(State.VALID, createComparison("number", "!=", "b").isValid(TestDataRecordFactory.addStringField(null, "number","a"), null));
		
		assertEquals(State.VALID, createComparison("number", ">=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","beta"), null));
		assertEquals(State.VALID, createComparison("number", ">=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","bfta"), null));
		assertEquals(State.INVALID, createComparison("number", ">=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","bet"), null));
		assertEquals(State.INVALID, createComparison("number", ">=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","alfa"), null));
		
		assertEquals(State.INVALID, createComparison("number", ">", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","beta"), null));
		assertEquals(State.VALID, createComparison("number", ">", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","bfta"), null));
		assertEquals(State.VALID, createComparison("number", ">", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","gama"), null));
		assertEquals(State.VALID, createComparison("number", ">", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","betaa"), null));
		assertEquals(State.INVALID, createComparison("number", ">", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","bet"), null));
		assertEquals(State.INVALID, createComparison("number", ">", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","alfa"), null));
		
		assertEquals(State.INVALID, createComparison("number", "<", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","beta"), null));
		assertEquals(State.INVALID, createComparison("number", "<", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","bfta"), null));
		assertEquals(State.INVALID, createComparison("number", "<", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","gama"), null));
		assertEquals(State.INVALID, createComparison("number", "<", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","betaa"), null));
		assertEquals(State.VALID, createComparison("number", "<", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","bet"), null));
		assertEquals(State.VALID, createComparison("number", "<", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","alfa"), null));
		
		assertEquals(State.VALID, createComparison("number", "<=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","beta"), null));
		assertEquals(State.VALID, createComparison("number", "<=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","bdta"), null));
		assertEquals(State.VALID, createComparison("number", "<=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","bet"), null));
		assertEquals(State.VALID, createComparison("number", "<=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","alfa"), null));
		assertEquals(State.INVALID, createComparison("number", "<=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","betaa"), null));
		assertEquals(State.INVALID, createComparison("number", "<=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","bfta"), null));
		assertEquals(State.INVALID, createComparison("number", "<=", "beta").isValid(TestDataRecordFactory.addStringField(null, "number","gama"), null));
	}
	public void testLongInLongComparison() {
		assertEquals(State.VALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addLongField(null, "number",10l), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addLongField(null, "number",11l), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addLongField(null, "number",12l), null));
		
		assertEquals(State.VALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addLongField(null, "number",9l), null));
		assertEquals(State.INVALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addLongField(null, "number",10l), null));
		assertEquals(State.VALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addLongField(null, "number",11l), null));
		
		assertEquals(State.VALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addLongField(null, "number",9l), null));
		assertEquals(State.VALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addLongField(null, "number",10l), null));
		assertEquals(State.INVALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addLongField(null, "number",11l), null));
		
		assertEquals(State.VALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addLongField(null, "number",9l), null));
		assertEquals(State.INVALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addLongField(null, "number",10l), null));
		assertEquals(State.INVALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addLongField(null, "number",11l), null));
		
		assertEquals(State.INVALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addLongField(null, "number",9l), null));
		assertEquals(State.VALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addLongField(null, "number",10l), null));
		assertEquals(State.VALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addLongField(null, "number",11l), null));
		
		assertEquals(State.INVALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addLongField(null, "number",9l), null));
		assertEquals(State.INVALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addLongField(null, "number",10l), null));
		assertEquals(State.VALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addLongField(null, "number",11l), null));	
	}
	public void testIntegerInIntegerComparison() {
		assertEquals(State.VALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",10), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",11), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",12), null));
		
		assertEquals(State.VALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",9), null));
		assertEquals(State.INVALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",10), null));
		assertEquals(State.VALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",11), null));
		
		assertEquals(State.VALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",9), null));
		assertEquals(State.VALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",10), null));
		assertEquals(State.INVALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",11), null));
		
		assertEquals(State.VALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",9), null));
		assertEquals(State.INVALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",10), null));
		assertEquals(State.INVALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",11), null));
		
		assertEquals(State.INVALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",9), null));
		assertEquals(State.VALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",10), null));
		assertEquals(State.VALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",11), null));
		
		assertEquals(State.INVALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",9), null));
		assertEquals(State.INVALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",10), null));
		assertEquals(State.VALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addIntegerField(null, "number",11), null));	
	}
	public void testNumberInNumberComparison() {
		assertEquals(State.VALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10d), null));
		assertEquals(State.VALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10.0d), null));
		assertEquals(State.VALID, createComparison("number", "==", "10.0").isValid(TestDataRecordFactory.addNumberField(null, "number",10d), null));
		assertEquals(State.VALID, createComparison("number", "==", "10.00").isValid(TestDataRecordFactory.addNumberField(null, "number",10d), null));
		assertEquals(State.VALID, createComparison("number", "==", "10.0").isValid(TestDataRecordFactory.addNumberField(null, "number",10.0d), null));
		assertEquals(State.VALID, createComparison("number", "==", "10.0").isValid(TestDataRecordFactory.addNumberField(null, "number",10.00d), null));
		assertEquals(State.VALID, createComparison("number", "==", "10.00").isValid(TestDataRecordFactory.addNumberField(null, "number",10.0d), null));
		assertEquals(State.VALID, createComparison("number", "==", "10.00").isValid(TestDataRecordFactory.addNumberField(null, "number",10.00d), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10.9d), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",11d), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",11.1d), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",12d), null));
		assertEquals(State.VALID, createComparison("number", "==", "10.34").isValid(TestDataRecordFactory.addNumberField(null, "number",10.34d), null));
		
		assertEquals(State.VALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",9d), null));
		assertEquals(State.INVALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10d), null));
		assertEquals(State.INVALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10.0d), null));
		assertEquals(State.INVALID, createComparison("number", "!=", "10.0").isValid(TestDataRecordFactory.addNumberField(null, "number",10.d), null));
		assertEquals(State.INVALID, createComparison("number", "!=", "10.0").isValid(TestDataRecordFactory.addNumberField(null, "number",10.0d), null));
		assertEquals(State.VALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10.1d), null));
		assertEquals(State.VALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",11d), null));
		
		assertEquals(State.VALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",9d), null));
		assertEquals(State.VALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10d), null));
		assertEquals(State.INVALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",11d), null));
		
		assertEquals(State.VALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",9d), null));
		assertEquals(State.INVALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10d), null));
		assertEquals(State.INVALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",11d), null));
		
		assertEquals(State.INVALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",9d), null));
		assertEquals(State.VALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10d), null));
		assertEquals(State.VALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",11d), null));
		
		assertEquals(State.INVALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",9d), null));
		assertEquals(State.INVALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",10d), null));
		assertEquals(State.VALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addNumberField(null, "number",11d), null));
	}
	public void testDecimalInDecimalComparison() {
		assertEquals(State.VALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.VALID, createComparison("number", "==", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.VALID, createComparison("number", "==", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.VALID, createComparison("number", "==", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("11.23")), null));
		assertEquals(State.INVALID, createComparison("number", "==", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("4.23")), null));
		
		assertEquals(State.INVALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.INVALID, createComparison("number", "!=", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.INVALID, createComparison("number", "!=", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.INVALID, createComparison("number", "!=", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.VALID, createComparison("number", "!=", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("11.23")), null));
		assertEquals(State.VALID, createComparison("number", "!=", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("4.23")), null));
		assertEquals(State.VALID, createComparison("number", "!=", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.1")), null));
		
		assertEquals(State.VALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.VALID, createComparison("number", "<=", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.VALID, createComparison("number", "<=", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.VALID, createComparison("number", "<=", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.INVALID, createComparison("number", "<=", "9.23").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.VALID, createComparison("number", "<=", "9.23").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("9.2222")), null));
		
		assertEquals(State.INVALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.INVALID, createComparison("number", "<", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.INVALID, createComparison("number", "<", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.INVALID, createComparison("number", "<", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.INVALID, createComparison("number", "<", "9.23").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.VALID, createComparison("number", "<", "9.23").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("9.2222")), null));
		
		assertEquals(State.VALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.VALID, createComparison("number", ">=", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.VALID, createComparison("number", ">=", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.VALID, createComparison("number", ">=", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.VALID, createComparison("number", ">=", "9.23").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.INVALID, createComparison("number", ">=", "9.23").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("9.2222")), null));
		
		assertEquals(State.INVALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.INVALID, createComparison("number", ">", "10").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.INVALID, createComparison("number", ">", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10.0")), null));
		assertEquals(State.INVALID, createComparison("number", ">", "10.0").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.VALID, createComparison("number", ">", "9.23").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("10")), null));
		assertEquals(State.INVALID, createComparison("number", ">", "9.23").isValid(TestDataRecordFactory.addDecimalField(null, "number",getDecimal("9.2222")), null));
	}
	
	public void testUserDataType() {
		// As strings
		assertEquals(State.INVALID, inType("s",createComparison("field", "==", "50")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("50.0")), null));
		
		// As longs
		assertEquals(State.INVALID, inType("l",createComparison("field", "==", "50")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("50")), null));
		assertEquals(State.VALID, inType("l",createComparison("field", "==", "50")).isValid(TestDataRecordFactory.addStringField(null, "field", "50"), null));
		assertEquals(State.INVALID, inType("l",createComparison("field", "==", "50.55")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("50.55")), null));
		
		// As decimals
		assertEquals(State.VALID, inType("d",createComparison("field", "==", "50.55")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("50.55")), null));
		assertEquals(State.VALID, inType("d",createComparison("field", "==", "50.55")).isValid(TestDataRecordFactory.addNumberField(null, "field", 50.55d), null));	
	}
	
	public void testDates() {
		assertEquals(State.VALID, inType("da", createComparison("field", "==", "2012-02-02", "yyyy-MM-dd", false)).isValid(TestDataRecordFactory.addStringField(null, "field","2012-02-02"), null));
		assertEquals(State.VALID, inType("da", createComparison("field", "==", "2012-02-02", "yyyy-MM-dd", false)).isValid(TestDataRecordFactory.addStringField(null, "field","2012-2-2"), null));
		assertEquals(State.INVALID, inType("da", createComparison("field", "==", "2012-02-02", "yyyy-MM-dd", false)).isValid(TestDataRecordFactory.addStringField(null, "field","2012-3-2"), null));
		assertEquals(State.VALID, inType("da", createComparison("field", "==", "2012-02-02", "yyyy-MM-dd", false)).isValid(TestDataRecordFactory.addStringField(null, "field","2012-02-2asdf"), null));
		assertEquals(State.INVALID, inType("da", createComparison("field", "==", "2012-02-02", "yyyy-MM-dd", true)).isValid(TestDataRecordFactory.addStringField(null, "field","2012-02-02asdf"), null));
	}
	
	/* Some helpers */
	private ComparisonValidationRule createComparison(String target, String operator, String value) {
		ComparisonValidationRule.OPERATOR_TYPE op = null;
		if(operator.equals("==")) {
			op = ComparisonValidationRule.OPERATOR_TYPE.E;
		} else if(operator.equals("!=")) {
			op = ComparisonValidationRule.OPERATOR_TYPE.NE;
		} else if(operator.equals("<=")) {
			op = ComparisonValidationRule.OPERATOR_TYPE.LE;
		} else if(operator.equals(">=")) {
			op = ComparisonValidationRule.OPERATOR_TYPE.GE;
		} else if(operator.equals("<")) {
			op = ComparisonValidationRule.OPERATOR_TYPE.L;
		} else if(operator.equals(">")) {
			op = ComparisonValidationRule.OPERATOR_TYPE.G;
		}
		ComparisonValidationRule rule = new ComparisonValidationRule();
		rule.setEnabled(true);
		rule.getTarget().setValue(target);
		rule.getOperator().setValue(op);
		rule.getValue().setValue(value);
		return rule;
	}
	private ComparisonValidationRule createComparison(String target, String operator, String value, String format, boolean strict) {
		ComparisonValidationRule rule = createComparison(target, operator, value);
		rule.getStrict().setValue(strict);
		rule.getFormat().setValue(format);
		return rule;
	}
	private ComparisonValidationRule inType(String type, ComparisonValidationRule rule) {
		ConversionValidationRule.METADATA_TYPES t = null;
		if(type.equals("s")) {
			t = ConversionValidationRule.METADATA_TYPES.STRING;
		} else if(type.equals("da")) {
			t = ConversionValidationRule.METADATA_TYPES.DATE;
		} else if(type.equals("l")) {
			t = ConversionValidationRule.METADATA_TYPES.LONG;
		} else if(type.equals("n")) {
			t = ConversionValidationRule.METADATA_TYPES.NUMBER;
		} else if(type.equals("d")) {
			t = ConversionValidationRule.METADATA_TYPES.DECIMAL;
		}
		rule.getUseType().setValue(t);
		return rule;
	}
	private Decimal getDecimal(String input) {
		return DecimalFactory.getDecimal(input);
	}

}
