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

import org.jetel.component.validator.common.ConversionTestCase;
import org.jetel.data.DataRecord;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 9.1.2013
 */
public class ComparisonValidationRuleTest extends ConversionTestCase {
	
	@Test
	public void testNameability() {
		testNameability(ComparisonValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(ComparisonValidationRule.class);
	}
	@Test
	public void testCommon() {
		testCommon(ComparisonValidationRule.class);
	}
	@Test
	public void testReadyness() {
		DataRecord record = RF.addStringField(null, "field", "");
		assertReadyness(false, newRule("", "==", "50"), record.getMetadata());	// Empty target
		assertReadyness(false, newRule("field2", "==", "50"), record.getMetadata());	// Non-existent target
		assertReadyness(true, newRule("field", "==", "50"), record.getMetadata());
		
		assertReadyness(false, newRule("field", "==", ""), record.getMetadata());	// Empty value
		
		assertReadyness(false, inType("da", df("wrong", newRule("field", "==", "50"))), record.getMetadata());
		assertReadyness(true, inType("da", df("yyyy-MM-dd", newRule("field", "==", "50"))), record.getMetadata());
		assertReadyness(true, inType("da", df("joda:yyyy-MM-dd", newRule("field", "==", "50"))), record.getMetadata());
		assertReadyness(true, inType("da", df("java:yyyy-MM-dd", newRule("field", "==", "50"))), record.getMetadata());
		
		ConversionValidationRule temp;
		temp = inType("da", newRule("field", "==", "50"));
		temp.getParentLanguageSetting().getDateFormat().setValue("");
		assertReadyness(false, temp, record.getMetadata());
		
		temp = inType("da", newRule("field", "==", "50"));
		temp.getParentLanguageSetting().getLocale().setValue("");
		assertReadyness(false, temp, record.getMetadata());
		
		temp = inType("da", newRule("field", "==", "50"));
		temp.getParentLanguageSetting().getTimezone().setValue("");
		assertReadyness(false, temp, record.getMetadata());
		
		temp = inType("d", newRule("field", "==", "50"));
		temp.getParentLanguageSetting().getLocale().setValue("");
		assertReadyness(false, temp, record.getMetadata());
	}
	
	@Test
	public void testStringInStringComparison() {
		assertValid(newRule("number", "==", "b"), RF.addStringField(null, "number","b"));
		assertInvalid(newRule("number", "==", "b"), RF.addStringField(null, "number","c"));
		assertInvalid(newRule("number", "==", "b"), RF.addStringField(null, "number","a"));
		
		assertInvalid(newRule("number", "!=", "b"), RF.addStringField(null, "number","b"));
		assertValid(newRule("number", "!=", "b"), RF.addStringField(null, "number","c"));
		assertValid(newRule("number", "!=", "b"), RF.addStringField(null, "number","a"));
		
		assertValid(newRule("number", ">=", "beta"), RF.addStringField(null, "number","beta"));
		assertValid(newRule("number", ">=", "beta"), RF.addStringField(null, "number","bfta"));
		assertInvalid(newRule("number", ">=", "beta"), RF.addStringField(null, "number","bet"));
		assertInvalid(newRule("number", ">=", "beta"), RF.addStringField(null, "number","alfa"));
		
		assertInvalid(newRule("number", ">", "beta"), RF.addStringField(null, "number","beta"));
		assertValid(newRule("number", ">", "beta"), RF.addStringField(null, "number","bfta"));
		assertValid(newRule("number", ">", "beta"), RF.addStringField(null, "number","gama"));
		assertValid(newRule("number", ">", "beta"), RF.addStringField(null, "number","betaa"));
		assertInvalid(newRule("number", ">", "beta"), RF.addStringField(null, "number","bet"));
		assertInvalid(newRule("number", ">", "beta"), RF.addStringField(null, "number","alfa"));
		
		assertInvalid(newRule("number", "<", "beta"), RF.addStringField(null, "number","beta"));
		assertInvalid(newRule("number", "<", "beta"), RF.addStringField(null, "number","bfta"));
		assertInvalid(newRule("number", "<", "beta"), RF.addStringField(null, "number","gama"));
		assertInvalid(newRule("number", "<", "beta"), RF.addStringField(null, "number","betaa"));
		assertValid(newRule("number", "<", "beta"), RF.addStringField(null, "number","bet"));
		assertValid(newRule("number", "<", "beta"), RF.addStringField(null, "number","alfa"));
		
		assertValid(newRule("number", "<=", "beta"), RF.addStringField(null, "number","beta"));
		assertValid(newRule("number", "<=", "beta"), RF.addStringField(null, "number","bdta"));
		assertValid(newRule("number", "<=", "beta"), RF.addStringField(null, "number","bet"));
		assertValid(newRule("number", "<=", "beta"), RF.addStringField(null, "number","alfa"));
		assertInvalid(newRule("number", "<=", "beta"), RF.addStringField(null, "number","betaa"));
		assertInvalid(newRule("number", "<=", "beta"), RF.addStringField(null, "number","bfta"));
		assertInvalid(newRule("number", "<=", "beta"), RF.addStringField(null, "number","gama"));
	}
	public void testLongInLongComparison() {
		assertValid(newRule("number", "==", "10"), RF.addLongField(null, "number",10l));
		assertInvalid(newRule("number", "==", "10"), RF.addLongField(null, "number",11l));
		assertInvalid(newRule("number", "==", "10"), RF.addLongField(null, "number",12l));
		
		assertValid(newRule("number", "!=", "10"), RF.addLongField(null, "number",9l));
		assertInvalid(newRule("number", "!=", "10"), RF.addLongField(null, "number",10l));
		assertValid(newRule("number", "!=", "10"), RF.addLongField(null, "number",11l));
		
		assertValid(newRule("number", "<=", "10"), RF.addLongField(null, "number",9l));
		assertValid(newRule("number", "<=", "10"), RF.addLongField(null, "number",10l));
		assertInvalid(newRule("number", "<=", "10"), RF.addLongField(null, "number",11l));
		
		assertValid(newRule("number", "<", "10"), RF.addLongField(null, "number",9l));
		assertInvalid(newRule("number", "<", "10"), RF.addLongField(null, "number",10l));
		assertInvalid(newRule("number", "<", "10"), RF.addLongField(null, "number",11l));
		
		assertInvalid(newRule("number", ">=", "10"), RF.addLongField(null, "number",9l));
		assertValid(newRule("number", ">=", "10"), RF.addLongField(null, "number",10l));
		assertValid(newRule("number", ">=", "10"), RF.addLongField(null, "number",11l));
		
		assertInvalid(newRule("number", ">", "10"), RF.addLongField(null, "number",9l));
		assertInvalid(newRule("number", ">", "10"), RF.addLongField(null, "number",10l));
		assertValid(newRule("number", ">", "10"), RF.addLongField(null, "number",11l));	
	}
	public void testIntegerInIntegerComparison() {
		assertValid(newRule("number", "==", "10"), RF.addIntegerField(null, "number",10));
		assertInvalid(newRule("number", "==", "10"), RF.addIntegerField(null, "number",11));
		assertInvalid(newRule("number", "==", "10"), RF.addIntegerField(null, "number",12));
		
		assertValid(newRule("number", "!=", "10"), RF.addIntegerField(null, "number",9));
		assertInvalid(newRule("number", "!=", "10"), RF.addIntegerField(null, "number",10));
		assertValid(newRule("number", "!=", "10"), RF.addIntegerField(null, "number",11));
		
		assertValid(newRule("number", "<=", "10"), RF.addIntegerField(null, "number",9));
		assertValid(newRule("number", "<=", "10"), RF.addIntegerField(null, "number",10));
		assertInvalid(newRule("number", "<=", "10"), RF.addIntegerField(null, "number",11));
		
		assertValid(newRule("number", "<", "10"), RF.addIntegerField(null, "number",9));
		assertInvalid(newRule("number", "<", "10"), RF.addIntegerField(null, "number",10));
		assertInvalid(newRule("number", "<", "10"), RF.addIntegerField(null, "number",11));
		
		assertInvalid(newRule("number", ">=", "10"), RF.addIntegerField(null, "number",9));
		assertValid(newRule("number", ">=", "10"), RF.addIntegerField(null, "number",10));
		assertValid(newRule("number", ">=", "10"), RF.addIntegerField(null, "number",11));
		
		assertInvalid(newRule("number", ">", "10"), RF.addIntegerField(null, "number",9));
		assertInvalid(newRule("number", ">", "10"), RF.addIntegerField(null, "number",10));
		assertValid(newRule("number", ">", "10"), RF.addIntegerField(null, "number",11));	
	}
	public void testNumberInNumberComparison() {
		assertValid(newRule("number", "==", "10"), RF.addNumberField(null, "number",10d));
		assertValid(newRule("number", "==", "10"), RF.addNumberField(null, "number",10.0d));
		assertValid(newRule("number", "==", "10.0"), RF.addNumberField(null, "number",10d));
		assertValid(newRule("number", "==", "10.00"), RF.addNumberField(null, "number",10d));
		assertValid(newRule("number", "==", "10.0"), RF.addNumberField(null, "number",10.0d));
		assertValid(newRule("number", "==", "10.0"), RF.addNumberField(null, "number",10.00d));
		assertValid(newRule("number", "==", "10.00"), RF.addNumberField(null, "number",10.0d));
		assertValid(newRule("number", "==", "10.00"), RF.addNumberField(null, "number",10.00d));
		assertInvalid(newRule("number", "==", "10"), RF.addNumberField(null, "number",10.9d));
		assertInvalid(newRule("number", "==", "10"), RF.addNumberField(null, "number",11d));
		assertInvalid(newRule("number", "==", "10"), RF.addNumberField(null, "number",11.1d));
		assertInvalid(newRule("number", "==", "10"), RF.addNumberField(null, "number",12d));
		assertValid(newRule("number", "==", "10.34"), RF.addNumberField(null, "number",10.34d));
		
		assertValid(newRule("number", "!=", "10"), RF.addNumberField(null, "number",9d));
		assertInvalid(newRule("number", "!=", "10"), RF.addNumberField(null, "number",10d));
		assertInvalid(newRule("number", "!=", "10"), RF.addNumberField(null, "number",10.0d));
		assertInvalid(newRule("number", "!=", "10.0"), RF.addNumberField(null, "number",10.d));
		assertInvalid(newRule("number", "!=", "10.0"), RF.addNumberField(null, "number",10.0d));
		assertValid(newRule("number", "!=", "10"), RF.addNumberField(null, "number",10.1d));
		assertValid(newRule("number", "!=", "10"), RF.addNumberField(null, "number",11d));
		
		assertValid(newRule("number", "<=", "10"), RF.addNumberField(null, "number",9d));
		assertValid(newRule("number", "<=", "10"), RF.addNumberField(null, "number",10d));
		assertInvalid(newRule("number", "<=", "10"), RF.addNumberField(null, "number",11d));
		
		assertValid(newRule("number", "<", "10"), RF.addNumberField(null, "number",9d));
		assertInvalid(newRule("number", "<", "10"), RF.addNumberField(null, "number",10d));
		assertInvalid(newRule("number", "<", "10"), RF.addNumberField(null, "number",11d));
		
		assertInvalid(newRule("number", ">=", "10"), RF.addNumberField(null, "number",9d));
		assertValid(newRule("number", ">=", "10"), RF.addNumberField(null, "number",10d));
		assertValid(newRule("number", ">=", "10"), RF.addNumberField(null, "number",11d));
		
		assertInvalid(newRule("number", ">", "10"), RF.addNumberField(null, "number",9d));
		assertInvalid(newRule("number", ">", "10"), RF.addNumberField(null, "number",10d));
		assertValid(newRule("number", ">", "10"), RF.addNumberField(null, "number",11d));
	}
	public void testDecimalInDecimalComparison() {
		assertValid(newRule("number", "==", "10"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertValid(newRule("number", "==", "10"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertValid(newRule("number", "==", "10.0"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertValid(newRule("number", "==", "10.0"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertInvalid(newRule("number", "==", "10.0"), RF.addDecimalField(null, "number",getDecimal("11.23")));
		assertInvalid(newRule("number", "==", "10.0"), RF.addDecimalField(null, "number",getDecimal("4.23")));
		
		assertInvalid(newRule("number", "!=", "10"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertInvalid(newRule("number", "!=", "10"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertInvalid(newRule("number", "!=", "10.0"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertInvalid(newRule("number", "!=", "10.0"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertValid(newRule("number", "!=", "10.0"), RF.addDecimalField(null, "number",getDecimal("11.23")));
		assertValid(newRule("number", "!=", "10.0"), RF.addDecimalField(null, "number",getDecimal("4.23")));
		assertValid(newRule("number", "!=", "10.0"), RF.addDecimalField(null, "number",getDecimal("10.1")));
		
		assertValid(newRule("number", "<=", "10"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertValid(newRule("number", "<=", "10"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertValid(newRule("number", "<=", "10.0"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertValid(newRule("number", "<=", "10.0"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertInvalid(newRule("number", "<=", "9.23"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertValid(newRule("number", "<=", "9.23"), RF.addDecimalField(null, "number",getDecimal("9.2222")));
		
		assertInvalid(newRule("number", "<", "10"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertInvalid(newRule("number", "<", "10"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertInvalid(newRule("number", "<", "10.0"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertInvalid(newRule("number", "<", "10.0"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertInvalid(newRule("number", "<", "9.23"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertValid(newRule("number", "<", "9.23"), RF.addDecimalField(null, "number",getDecimal("9.2222")));
		
		assertValid(newRule("number", ">=", "10"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertValid(newRule("number", ">=", "10"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertValid(newRule("number", ">=", "10.0"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertValid(newRule("number", ">=", "10.0"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertValid(newRule("number", ">=", "9.23"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertInvalid(newRule("number", ">=", "9.23"), RF.addDecimalField(null, "number",getDecimal("9.2222")));
		
		assertInvalid(newRule("number", ">", "10"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertInvalid(newRule("number", ">", "10"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertInvalid(newRule("number", ">", "10.0"), RF.addDecimalField(null, "number",getDecimal("10.0")));
		assertInvalid(newRule("number", ">", "10.0"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertValid(newRule("number", ">", "9.23"), RF.addDecimalField(null, "number",getDecimal("10")));
		assertInvalid(newRule("number", ">", "9.23"), RF.addDecimalField(null, "number",getDecimal("9.2222")));
	}
	
	public void testUserDataType() {
		// As strings
		assertInvalid(inType("s",newRule("field", "==", "50")), RF.addDecimalField(null, "field", getDecimal("50.0")));
		
		// As longs
		assertInvalid(inType("l",newRule("field", "==", "50")), RF.addDecimalField(null, "field", getDecimal("50")));
		assertValid(inType("l",newRule("field", "==", "50")), RF.addStringField(null, "field", "50"));
		assertInvalid(inType("l",newRule("field", "==", "50.55")), RF.addDecimalField(null, "field", getDecimal("50.55")));
		
		// As decimals
		assertValid(inType("d",newRule("field", "==", "50.55")), RF.addDecimalField(null, "field", getDecimal("50.55")));
		assertValid(inType("d",newRule("field", "==", "50.55")), RF.addNumberField(null, "field", 50.55d));	
	}
	
	public void testDates() {
		assertValid(inType("da", df("yyyy-MM-dd", newRule("field", "==", "2012-02-02 00:00:00"))), RF.addStringField(null, "field","2012-02-02"));
		assertValid(inType("da", df("yyyy-MM-dd", newRule("field", "==", "2012-02-02"))), RF.addStringField(null, "field","2012-02-02"));
		assertValid(inType("da", lo("cs.CZ", df("yyyy-MM-dd", newRule("field", "==", "2012-02-02 00:00:00")))), RF.addStringField(null, "field","2012-02-02"));
		
		assertValid(inType("da", df("yyyy-MM-dd hh:mm:ss", newRule("field", "==", "2012-02-02 10:20:30"))), RF.addStringField(null, "field","2012-02-02 10:20:30"));
		
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "==", "2012-02-02 00:00:00"))), RF.addStringField(null, "field","2012-2-2"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "==", "2012-02-02 00:00:00"))), RF.addStringField(null, "field","2012-3-2"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "==", "2012-02-02 00:00:00"))), RF.addStringField(null, "field","2012-02-2asdf"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "==", "2012-02-02 00:00:00"))), RF.addStringField(null, "field","2012-02-02asdf"));
		
		assertValid(inType("da", df("yyyy-MM-dd", newRule("field", "<=", "2012-02-03 01:00:00"))), RF.addStringField(null, "field","2012-02-03"));
		assertValid(inType("da", df("yyyy-MM-dd", newRule("field", "<=", "2012-02-04 00:00:00"))), RF.addStringField(null, "field","2012-02-03"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "<=", "2012-02-02 23:59:59"))), RF.addStringField(null, "field","2012-02-03"));
	}
	
	public void testLocale() {
		assertValid(lo("cs.CZ", inType("d",newRule("field", "==", "50.55"))), RF.addStringField(null, "field", "50,55"));
		assertInvalid(lo("cs.CZ", inType("d",newRule("field", "==", "50"))), RF.addStringField(null, "field", "50,55"));
		assertValid(inType("d",newRule("field", "==", "50.55")), RF.addNumberField(null, "field", 50.55d));

		assertInvalid(inType("d",lo("en.GB", nf("#", newRule("field", "==", "50.55")))), RF.addStringField(null, "field", "50,55"));
		
		assertValid(inType("d",lo("cs.CZ", nf("#", newRule("field", "==", "50.55")))), RF.addStringField(null, "field", "50,55"));
		
		assertInvalid(inType("d",lo("en.GB", nf("#", newRule("field", "==", "50.55")))), RF.addStringField(null, "field", "50,55"));
		
		assertValid(inType("d",lo("cs.CZ", nf("#", newRule("field", "==", "50.55")))), RF.addStringField(null, "field", "50,55"));
	}
	
	private ComparisonValidationRule newRule(String target, String operator, String value) {
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
		ComparisonValidationRule rule = createRule(ComparisonValidationRule.class);
		rule.setEnabled(true);
		rule.getTarget().setValue(target);
		rule.getOperator().setValue(op);
		rule.getValue().setValue(value);
		return rule;
	}

}
