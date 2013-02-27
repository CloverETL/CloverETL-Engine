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
public class RangeCheckValidationRuleTest extends ValidatorTestCase {
	
	private static final String TARGET = "target";
	private static final String TYPE = "type";
	private static final String OPERATOR= "operator";
	private static final String BOUNDARIES = "boundaries";
	private static final String FROM = "from";
	private static final String TO = "to";
	private static final String VALUE = "value";
	private static final String USE_TYPE = "useType";
	
	@Test
	public void testNameability() {
		testNameability(RangeCheckValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(RangeCheckValidationRule.class);
	}
	@Test
	public void testAttributes() {
		testStringAttribute(RangeCheckValidationRule.class, TARGET, "");
		testEnumAttribute(RangeCheckValidationRule.class, TYPE, RangeCheckValidationRule.TYPES.values(), RangeCheckValidationRule.TYPES.COMPARISON);
		
		testEnumAttribute(RangeCheckValidationRule.class, OPERATOR, RangeCheckValidationRule.OPERATOR_TYPE.values(), RangeCheckValidationRule.OPERATOR_TYPE.E);
		testStringAttribute(RangeCheckValidationRule.class, VALUE, "");
		
		testEnumAttribute(RangeCheckValidationRule.class, BOUNDARIES, RangeCheckValidationRule.BOUNDARIES_TYPE.values(), RangeCheckValidationRule.BOUNDARIES_TYPE.CLOSED_CLOSED);
		testStringAttribute(RangeCheckValidationRule.class, FROM, "");
		testStringAttribute(RangeCheckValidationRule.class, TO, "");
		
		testEnumAttribute(RangeCheckValidationRule.class, USE_TYPE, RangeCheckValidationRule.METADATA_TYPES.values(), RangeCheckValidationRule.METADATA_TYPES.DEFAULT);		
	}
	@Test
	public void testReadynessComparison() {
		RangeCheckValidationRule rule = new RangeCheckValidationRule();
		assertFalse(rule.isReady());
		
		// TBD: tests
	}
	@Test
	public void testReadynessInterval() {
		RangeCheckValidationRule rule = new RangeCheckValidationRule();
		assertFalse(rule.isReady());
		
		// TBD: tests
	}
	
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
	@Test
	public void testStringInStringInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "ce"), null));
		assertEquals(State.VALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "cep"), null));
		assertEquals(State.VALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "noha"), null));
		assertEquals(State.VALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "opera"), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "operaa"), null));
		
		assertEquals(State.INVALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "ce"), null));
		assertEquals(State.VALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "cep"), null));
		assertEquals(State.VALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "noha"), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "opera"), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "operaa"), null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "ce"), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "cep"), null));
		assertEquals(State.VALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "noha"), null));
		assertEquals(State.VALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "opera"), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "operaa"), null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "ce"), null));
		assertEquals(State.INVALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "cep"), null));
		assertEquals(State.VALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "noha"), null));
		assertEquals(State.INVALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "opera"), null));
		assertEquals(State.INVALID, createInterval("field", "()", "cep", "opera").isValid(TestDataRecordFactory.addStringField(null, "field", "operaa"), null));
	}
	@Test
	public void testLongInLongInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 4l), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 5l), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 8l), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 9l), null));
		
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 4l), null));
		assertEquals(State.VALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 5l), null));
		assertEquals(State.VALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 8l), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 9l), null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 4l), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 5l), null));
		assertEquals(State.VALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null));
		assertEquals(State.VALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 8l), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 9l), null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 4l), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 5l), null));
		assertEquals(State.VALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 8l), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 9l), null));
	}
	@Test
	public void testIntegerInIntegerInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 4), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 5), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 8), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 9), null));
		
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 4), null));
		assertEquals(State.VALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 5), null));
		assertEquals(State.VALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 8), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 9), null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 4), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 5), null));
		assertEquals(State.VALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null));
		assertEquals(State.VALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 8), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 9), null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 4), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 5), null));
		assertEquals(State.VALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 8), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5", "8").isValid(TestDataRecordFactory.addIntegerField(null, "field", 9), null));
	}
	@Test
	public void testNumberInNumberInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5d), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5.66d), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.85d), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.9d), null));

		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5d), null));
		assertEquals(State.VALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5.66d), null));
		assertEquals(State.VALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.85d), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.9d), null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5d), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5.66d), null));
		assertEquals(State.VALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null));
		assertEquals(State.VALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.85d), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.9d), null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5d), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 5.66d), null));
		assertEquals(State.VALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.85d), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 7.9d), null));
	}
	@Test
	public void testDecimalInDecimalInterval() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5")), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null));
		assertEquals(State.VALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.85")), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.9")), null));

		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5")), null));
		assertEquals(State.VALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null));
		assertEquals(State.VALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.85")), null));
		assertEquals(State.INVALID, createInterval("field", "[)", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.9")), null));
		
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5")), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null));
		assertEquals(State.VALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null));
		assertEquals(State.VALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.85")), null));
		assertEquals(State.INVALID, createInterval("field", "(]", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.9")), null));
		
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5")), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null));
		assertEquals(State.VALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.85")), null));
		assertEquals(State.INVALID, createInterval("field", "()", "5.66", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7.9")), null));
	}
	
	@Test
	public void testInvalidInputs() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66A", "7.85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66A", "7,85").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("6.76")), null));
		
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66a", "7.85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.dd85").isValid(TestDataRecordFactory.addNumberField(null, "field", 6.76d), null));
		
		assertEquals(State.INVALID, createInterval("field", "[]", "a5", "8").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8,0").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5", "8.0").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null));
	}
	@Test
	public void testNonConvertible() {
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addIntegerField(null, "field", 7), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "5.66", "7.85").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "a", "s").isValid(TestDataRecordFactory.addLongField(null, "field", 7l), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "a", "s").isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("7")), null));
		assertEquals(State.INVALID, createInterval("field", "[]", "a", "s").isValid(TestDataRecordFactory.addNumberField(null, "field", 7d), null));
	}
	
	public void testUserDataType() {
		// As strings
		assertEquals(State.VALID, inType("s",createInterval("field", "[]", "5.66", "7.85")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("5.66")), null));
		assertEquals(State.INVALID, inType("s",createInterval("field", "[]", "5.66", "100.85")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("10.66")), null));
		assertEquals(State.INVALID, inType("s",createComparison("field", "==", "50")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("50.0")), null));
		
		// As longs
		assertEquals(State.INVALID, inType("l",createComparison("field", "==", "50")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("50")), null));
		assertEquals(State.VALID, inType("l",createComparison("field", "==", "50")).isValid(TestDataRecordFactory.addStringField(null, "field", "50"), null));
		assertEquals(State.INVALID, inType("l",createComparison("field", "==", "50.55")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("50.55")), null));
		
		// As decimals
		assertEquals(State.VALID, inType("d",createComparison("field", "==", "50.55")).isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("50.55")), null));
		assertEquals(State.VALID, inType("d",createComparison("field", "==", "50.55")).isValid(TestDataRecordFactory.addNumberField(null, "field", 50.55d), null));
		
	}
	
	/* Some helpers */
	private RangeCheckValidationRule createComparison(String target, String operator, String value) {
		RangeCheckValidationRule.OPERATOR_TYPE op = null;
		if(operator.equals("==")) {
			op = RangeCheckValidationRule.OPERATOR_TYPE.E;
		} else if(operator.equals("!=")) {
			op = RangeCheckValidationRule.OPERATOR_TYPE.NE;
		} else if(operator.equals("<=")) {
			op = RangeCheckValidationRule.OPERATOR_TYPE.LE;
		} else if(operator.equals(">=")) {
			op = RangeCheckValidationRule.OPERATOR_TYPE.GE;
		} else if(operator.equals("<")) {
			op = RangeCheckValidationRule.OPERATOR_TYPE.L;
		} else if(operator.equals(">")) {
			op = RangeCheckValidationRule.OPERATOR_TYPE.G;
		}
		RangeCheckValidationRule rule = new RangeCheckValidationRule();
		rule.setEnabled(true);
		setEnumParam(rule,TYPE, RangeCheckValidationRule.TYPES.COMPARISON);
		setStringParam(rule, TARGET, target);
		setEnumParam(rule,OPERATOR,op);
		setStringParam(rule, VALUE, value);
		return rule;
	}
	private RangeCheckValidationRule createInterval(String target, String interval, String from, String to) {
		RangeCheckValidationRule.BOUNDARIES_TYPE b = null;
		if(interval.equals("[]")) {
			b = RangeCheckValidationRule.BOUNDARIES_TYPE.CLOSED_CLOSED;
		} else if(interval.equals("[)")) {
			b = RangeCheckValidationRule.BOUNDARIES_TYPE.CLOSED_OPEN;
		} else if(interval.equals("(]")) {
			b = RangeCheckValidationRule.BOUNDARIES_TYPE.OPEN_CLOSED;
		} else if(interval.equals("()")) {
			b = RangeCheckValidationRule.BOUNDARIES_TYPE.OPEN_OPEN;
		}
		RangeCheckValidationRule rule = new RangeCheckValidationRule();
		rule.setEnabled(true);
		setEnumParam(rule,TYPE, RangeCheckValidationRule.TYPES.INTERVAL);
		setStringParam(rule, TARGET, target);
		setEnumParam(rule, BOUNDARIES, b);
		setStringParam(rule, FROM, from);
		setStringParam(rule, TO, to);
		return rule;
	}
	
	private RangeCheckValidationRule inType(String type, RangeCheckValidationRule rule) {
		RangeCheckValidationRule.METADATA_TYPES t = null;
		if(type.equals("s")) {
			t = RangeCheckValidationRule.METADATA_TYPES.STRING;
		} else if(type.equals("l")) {
			t = RangeCheckValidationRule.METADATA_TYPES.LONG;
		} else if(type.equals("n")) {
			t = RangeCheckValidationRule.METADATA_TYPES.NUMBER;
		} else if(type.equals("d")) {
			t = RangeCheckValidationRule.METADATA_TYPES.DECIMAL;
		}
		setEnumParam(rule, USE_TYPE, t);
		return rule;
	}
	private Decimal getDecimal(String input) {
		return DecimalFactory.getDecimal(input);
	}

}
