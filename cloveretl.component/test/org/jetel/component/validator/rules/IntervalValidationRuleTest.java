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
public class IntervalValidationRuleTest extends ConversionTestCase {
	
	@Test
	public void testNameability() {
		testNameability(IntervalValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(IntervalValidationRule.class);
	}
	@Test
	public void testCommon() {
		testCommon(IntervalValidationRule.class);
	}
	
	@Test
	public void testReadyness() {
		DataRecord record = RF.addStringField(null, "field", "");
		assertReadyness(false, newRule("", "==", "50", "60"), record.getMetadata());	// Empty target
		assertReadyness(false, newRule("field2", "==", "50", "60"), record.getMetadata());	// Non-existent target
		assertReadyness(true, newRule("field", "==", "50", "60"), record.getMetadata());
		
		assertReadyness(false, newRule("field", "==", "", "60"), record.getMetadata());	// Empty from
		assertReadyness(false, newRule("field", "==", "50", ""), record.getMetadata());	// Empty to
		assertReadyness(false, newRule("field", "==", "", ""), record.getMetadata());	// Empty from and to
		
		assertReadyness(false, inType("da", df("wrong", newRule("field", "==", "50", "60"))), record.getMetadata());
		assertReadyness(true, inType("da", df("yyyy-MM-dd", newRule("field", "==", "50", "60"))), record.getMetadata());
		assertReadyness(true, inType("da", df("joda:yyyy-MM-dd", newRule("field", "==", "50", "60"))), record.getMetadata());
		assertReadyness(true, inType("da", df("java:yyyy-MM-dd", newRule("field", "==", "50", "60"))), record.getMetadata());
		
		ConversionValidationRule temp;
		temp = inType("da", newRule("field", "==", "50", "60"));
		temp.getParentLanguageSetting().getDateFormat().setValue("");
		assertReadyness(false, temp, record.getMetadata());
		
		temp = inType("da", newRule("field", "==", "50", "60"));
		temp.getParentLanguageSetting().getLocale().setValue("");
		assertReadyness(false, temp, record.getMetadata());
		
		temp = inType("da", newRule("field", "==", "50", "60"));
		temp.getParentLanguageSetting().getTimezone().setValue("");
		assertReadyness(false, temp, record.getMetadata());
		
		temp = inType("d", newRule("field", "==", "50", "60"));
		temp.getParentLanguageSetting().getLocale().setValue("");
		assertReadyness(false, temp, record.getMetadata());
	}

	@Test
	public void testStringInStringInterval() {
		assertInvalid(newRule("field", "[]", "cep", "opera"), RF.addStringField(null, "field", "ce"));
		assertValid(newRule("field", "[]", "cep", "opera"), RF.addStringField(null, "field", "cep"));
		assertValid(newRule("field", "[]", "cep", "opera"), RF.addStringField(null, "field", "noha"));
		assertValid(newRule("field", "[]", "cep", "opera"), RF.addStringField(null, "field", "opera"));
		assertInvalid(newRule("field", "[]", "cep", "opera"), RF.addStringField(null, "field", "operaa"));
		
		assertInvalid(newRule("field", "[)", "cep", "opera"), RF.addStringField(null, "field", "ce"));
		assertValid(newRule("field", "[)", "cep", "opera"), RF.addStringField(null, "field", "cep"));
		assertValid(newRule("field", "[)", "cep", "opera"), RF.addStringField(null, "field", "noha"));
		assertInvalid(newRule("field", "[)", "cep", "opera"), RF.addStringField(null, "field", "opera"));
		assertInvalid(newRule("field", "[)", "cep", "opera"), RF.addStringField(null, "field", "operaa"));
		
		assertInvalid(newRule("field", "(]", "cep", "opera"), RF.addStringField(null, "field", "ce"));
		assertInvalid(newRule("field", "(]", "cep", "opera"), RF.addStringField(null, "field", "cep"));
		assertValid(newRule("field", "(]", "cep", "opera"), RF.addStringField(null, "field", "noha"));
		assertValid(newRule("field", "(]", "cep", "opera"), RF.addStringField(null, "field", "opera"));
		assertInvalid(newRule("field", "(]", "cep", "opera"), RF.addStringField(null, "field", "operaa"));
		
		assertInvalid(newRule("field", "()", "cep", "opera"), RF.addStringField(null, "field", "ce"));
		assertInvalid(newRule("field", "()", "cep", "opera"), RF.addStringField(null, "field", "cep"));
		assertValid(newRule("field", "()", "cep", "opera"), RF.addStringField(null, "field", "noha"));
		assertInvalid(newRule("field", "()", "cep", "opera"), RF.addStringField(null, "field", "opera"));
		assertInvalid(newRule("field", "()", "cep", "opera"), RF.addStringField(null, "field", "operaa"));
	}
	@Test
	public void testLongInLongInterval() {
		assertInvalid(newRule("field", "[]", "5", "8"), RF.addLongField(null, "field", 4l));
		assertValid(newRule("field", "[]", "5", "8"), RF.addLongField(null, "field", 5l));
		assertValid(newRule("field", "[]", "5", "8"), RF.addLongField(null, "field", 7l));
		assertValid(newRule("field", "[]", "5", "8"), RF.addLongField(null, "field", 8l));
		assertInvalid(newRule("field", "[]", "5", "8"), RF.addLongField(null, "field", 9l));
		
		assertInvalid(newRule("field", "[)", "5", "8"), RF.addLongField(null, "field", 4l));
		assertValid(newRule("field", "[)", "5", "8"), RF.addLongField(null, "field", 5l));
		assertValid(newRule("field", "[)", "5", "8"), RF.addLongField(null, "field", 7l));
		assertInvalid(newRule("field", "[)", "5", "8"), RF.addLongField(null, "field", 8l));
		assertInvalid(newRule("field", "[)", "5", "8"), RF.addLongField(null, "field", 9l));
		
		assertInvalid(newRule("field", "(]", "5", "8"), RF.addLongField(null, "field", 4l));
		assertInvalid(newRule("field", "(]", "5", "8"), RF.addLongField(null, "field", 5l));
		assertValid(newRule("field", "(]", "5", "8"), RF.addLongField(null, "field", 7l));
		assertValid(newRule("field", "(]", "5", "8"), RF.addLongField(null, "field", 8l));
		assertInvalid(newRule("field", "(]", "5", "8"), RF.addLongField(null, "field", 9l));
		
		assertInvalid(newRule("field", "()", "5", "8"), RF.addLongField(null, "field", 4l));
		assertInvalid(newRule("field", "()", "5", "8"), RF.addLongField(null, "field", 5l));
		assertValid(newRule("field", "()", "5", "8"), RF.addLongField(null, "field", 7l));
		assertInvalid(newRule("field", "()", "5", "8"), RF.addLongField(null, "field", 8l));
		assertInvalid(newRule("field", "()", "5", "8"), RF.addLongField(null, "field", 9l));
	}
	@Test
	public void testIntegerInIntegerInterval() {
		assertInvalid(newRule("field", "[]", "5", "8"), RF.addIntegerField(null, "field", 4));
		assertValid(newRule("field", "[]", "5", "8"), RF.addIntegerField(null, "field", 5));
		assertValid(newRule("field", "[]", "5", "8"), RF.addIntegerField(null, "field", 7));
		assertValid(newRule("field", "[]", "5", "8"), RF.addIntegerField(null, "field", 8));
		assertInvalid(newRule("field", "[]", "5", "8"), RF.addIntegerField(null, "field", 9));
		
		assertInvalid(newRule("field", "[)", "5", "8"), RF.addIntegerField(null, "field", 4));
		assertValid(newRule("field", "[)", "5", "8"), RF.addIntegerField(null, "field", 5));
		assertValid(newRule("field", "[)", "5", "8"), RF.addIntegerField(null, "field", 7));
		assertInvalid(newRule("field", "[)", "5", "8"), RF.addIntegerField(null, "field", 8));
		assertInvalid(newRule("field", "[)", "5", "8"), RF.addIntegerField(null, "field", 9));
		
		assertInvalid(newRule("field", "(]", "5", "8"), RF.addIntegerField(null, "field", 4));
		assertInvalid(newRule("field", "(]", "5", "8"), RF.addIntegerField(null, "field", 5));
		assertValid(newRule("field", "(]", "5", "8"), RF.addIntegerField(null, "field", 7));
		assertValid(newRule("field", "(]", "5", "8"), RF.addIntegerField(null, "field", 8));
		assertInvalid(newRule("field", "(]", "5", "8"), RF.addIntegerField(null, "field", 9));
		
		assertInvalid(newRule("field", "()", "5", "8"), RF.addIntegerField(null, "field", 4));
		assertInvalid(newRule("field", "()", "5", "8"), RF.addIntegerField(null, "field", 5));
		assertValid(newRule("field", "()", "5", "8"), RF.addIntegerField(null, "field", 7));
		assertInvalid(newRule("field", "()", "5", "8"), RF.addIntegerField(null, "field", 8));
		assertInvalid(newRule("field", "()", "5", "8"), RF.addIntegerField(null, "field", 9));
	}
	@Test
	public void testNumberInNumberInterval() {
		assertInvalid(newRule("field", "[]", "5.66", "7.85"), RF.addNumberField(null, "field", 5d));
		assertValid(newRule("field", "[]", "5.66", "7.85"), RF.addNumberField(null, "field", 5.66d));
		assertValid(newRule("field", "[]", "5.66", "7.85"), RF.addNumberField(null, "field", 6.76d));
		assertValid(newRule("field", "[]", "5.66", "7.85"), RF.addNumberField(null, "field", 7.85d));
		assertInvalid(newRule("field", "[]", "5.66", "7.85"), RF.addNumberField(null, "field", 7.9d));

		assertInvalid(newRule("field", "[)", "5.66", "7.85"), RF.addNumberField(null, "field", 5d));
		assertValid(newRule("field", "[)", "5.66", "7.85"), RF.addNumberField(null, "field", 5.66d));
		assertValid(newRule("field", "[)", "5.66", "7.85"), RF.addNumberField(null, "field", 6.76d));
		assertInvalid(newRule("field", "[)", "5.66", "7.85"), RF.addNumberField(null, "field", 7.85d));
		assertInvalid(newRule("field", "[)", "5.66", "7.85"), RF.addNumberField(null, "field", 7.9d));
		
		assertInvalid(newRule("field", "(]", "5.66", "7.85"), RF.addNumberField(null, "field", 5d));
		assertInvalid(newRule("field", "(]", "5.66", "7.85"), RF.addNumberField(null, "field", 5.66d));
		assertValid(newRule("field", "(]", "5.66", "7.85"), RF.addNumberField(null, "field", 6.76d));
		assertValid(newRule("field", "(]", "5.66", "7.85"), RF.addNumberField(null, "field", 7.85d));
		assertInvalid(newRule("field", "(]", "5.66", "7.85"), RF.addNumberField(null, "field", 7.9d));
		
		assertInvalid(newRule("field", "()", "5.66", "7.85"), RF.addNumberField(null, "field", 5d));
		assertInvalid(newRule("field", "()", "5.66", "7.85"), RF.addNumberField(null, "field", 5.66d));
		assertValid(newRule("field", "()", "5.66", "7.85"), RF.addNumberField(null, "field", 6.76d));
		assertInvalid(newRule("field", "()", "5.66", "7.85"), RF.addNumberField(null, "field", 7.85d));
		assertInvalid(newRule("field", "()", "5.66", "7.85"), RF.addNumberField(null, "field", 7.9d));
	}
	@Test
	public void testDecimalInDecimalInterval() {
		assertInvalid(newRule("field", "[]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("5")));
		assertValid(newRule("field", "[]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("5.66")));
		assertValid(newRule("field", "[]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("6.76")));
		assertValid(newRule("field", "[]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("7.85")));
		assertInvalid(newRule("field", "[]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("7.9")));

		assertInvalid(newRule("field", "[)", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("5")));
		assertValid(newRule("field", "[)", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("5.66")));
		assertValid(newRule("field", "[)", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("6.76")));
		assertInvalid(newRule("field", "[)", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("7.85")));
		assertInvalid(newRule("field", "[)", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("7.9")));
		
		assertInvalid(newRule("field", "(]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("5")));
		assertInvalid(newRule("field", "(]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("5.66")));
		assertValid(newRule("field", "(]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("6.76")));
		assertValid(newRule("field", "(]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("7.85")));
		assertInvalid(newRule("field", "(]", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("7.9")));
		
		assertInvalid(newRule("field", "()", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("5")));
		assertInvalid(newRule("field", "()", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("5.66")));
		assertValid(newRule("field", "()", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("6.76")));
		assertInvalid(newRule("field", "()", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("7.85")));
		assertInvalid(newRule("field", "()", "5.66", "7.85"), RF.addDecimalField(null, "field", getDecimal("7.9")));
	}
	
	@Test
	public void testInvalidInputs() {
		assertInvalid(newRule("field", "[]", "5.66A", "7.85"), RF.addDecimalField(null, "field", getDecimal("6.76")));
		assertInvalid(newRule("field", "[]", "5.66A", "7,85"), RF.addDecimalField(null, "field", getDecimal("6.76")));
		
		assertInvalid(newRule("field", "[]", "5.66a", "7.85"), RF.addNumberField(null, "field", 6.76d));
		assertInvalid(newRule("field", "[]", "5.66", "7.dd85"), RF.addNumberField(null, "field", 6.76d));
		
		assertInvalid(newRule("field", "[]", "a5", "8"), RF.addLongField(null, "field", 7l));
		assertInvalid(newRule("field", "[]", "5", "8,0"), RF.addLongField(null, "field", 7l));
		assertInvalid(newRule("field", "[]", "5", "8.0"), RF.addLongField(null, "field", 7l));
	}
	@Test
	public void testNonConvertible() {
		assertInvalid(newRule("field", "[]", "5.66", "7.85"), RF.addIntegerField(null, "field", 7));
		assertInvalid(newRule("field", "[]", "5.66", "7.85"), RF.addLongField(null, "field", 7l));
		assertInvalid(newRule("field", "[]", "a", "s"), RF.addLongField(null, "field", 7l));
		assertInvalid(newRule("field", "[]", "a", "s"), RF.addDecimalField(null, "field", getDecimal("7")));
		assertInvalid(newRule("field", "[]", "a", "s"), RF.addNumberField(null, "field", 7d));
	}
	
	public void testUserSelectedType() {
		// As strings
		assertValid(inType("s",newRule("field", "[]", "5.66", "7.85")), RF.addDecimalField(null, "field", getDecimal("5.66")));
		assertInvalid(inType("s",newRule("field", "[]", "5.66", "100.85")), RF.addDecimalField(null, "field", getDecimal("10.66")));
		
		// As longs
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 9));
		assertValid(inType("l",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 10));
		assertValid(inType("l",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 12));
		assertValid(inType("l",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 13));
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 14));
		
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "9"));
		assertValid(inType("l",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "10"));
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "10.0"));
		assertValid(inType("l",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "12"));
		assertValid(inType("l",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "13"));
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "14"));
		
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("9")));	// Decimals are not supposed to be converted to long
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("10")));
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("10.0")));
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("12")));
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("13")));
		assertInvalid(inType("l",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("14")));
		
		// As decimals
		assertInvalid(inType("d",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 9));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 10));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 12));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 13));
		assertInvalid(inType("d",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 14));
		
		assertInvalid(inType("d",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "9"));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "10"));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "10.0"));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "12"));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "13"));
		assertInvalid(inType("d",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "14"));
		
		assertInvalid(inType("d",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("9")));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("10")));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("10.0")));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("12")));
		assertValid(inType("d",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("13")));
		assertInvalid(inType("d",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("14")));
		
		// As numbers
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 9));
		assertValid(inType("n",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 10));
		assertValid(inType("n",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 12));
		assertValid(inType("n",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 13));
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addIntegerField(null, "field", 14));
		
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "9"));
		assertValid(inType("n",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "10"));
		assertValid(inType("n",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "10.0"));
		assertValid(inType("n",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "12"));
		assertValid(inType("n",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "13"));
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addStringField(null, "field", "14"));
		
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("9"))); // Decimals are not supposed to be compared as numbers
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("10")));
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("10.0")));
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("12")));
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("13")));
		assertInvalid(inType("n",newRule("field", "[]", "10", "13")), RF.addDecimalField(null, "field", getDecimal("14")));
		
	}
	
	public void testDates() {
		assertValid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06", "2012-01-28"))), RF.addStringField(null, "field", "2012-01-17"));
		assertValid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06", "2012-01-28"))), RF.addStringField(null, "field", "2012-01-28"));
		assertValid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06", "2012-01-28"))), RF.addStringField(null, "field", "2012-01-06"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06", "2012-01-28"))), RF.addStringField(null, "field", "2012-0100-06"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06", "2012-01-28"))), RF.addStringField(null, "field", "2012-01a-17"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06", "2012-01-28"))), RF.addStringField(null, "field", "2012-01-05"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06", "2012-01-28"))), RF.addStringField(null, "field", "2012-01-17d"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06", "2012-01-28"))), RF.addStringField(null, "field", "2012-01-28dd"));
		assertInvalid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06", "2012-01-28"))), RF.addStringField(null, "field", "2012-01-06asd"));
		
		assertInvalid(inType("da", df("yyyy-MM-dd HH:mm:ss", newRule("field", "[]", "2012-01-06 10:10:10", "2012-01-17 20:10:10"))), RF.addStringField(null, "field", "2012-01-05 10:20:30"));
		assertInvalid(inType("da", df("yyyy-MM-dd HH:mm:ss", newRule("field", "[]", "2012-01-06 10:10:10", "2012-01-17 20:10:10"))), RF.addStringField(null, "field", "2012-01-17 21:20:30"));
		assertValid(inType("da", df("yyyy-MM-dd HH:mm:ss", newRule("field", "[]", "2012-01-06 10:10:10", "2012-01-17 20:10:10"))), RF.addStringField(null, "field", "2012-01-07 10:20:30"));
		assertValid(inType("da", df("yyyy-MM-dd", newRule("field", "[]", "2012-01-06 10:10:10", "2012-01-17 20:10:10"))), RF.addStringField(null, "field", "2012-01-17"));
	}
	
	private IntervalValidationRule newRule(String target, String interval, String from, String to) {
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
		IntervalValidationRule rule = createRule(IntervalValidationRule.class);
		rule.getTarget().setValue(target);
		rule.getBoundaries().setValue(b);
		rule.getFrom().setValue(from);
		rule.getTo().setValue(to);
		return rule;
	}
}
