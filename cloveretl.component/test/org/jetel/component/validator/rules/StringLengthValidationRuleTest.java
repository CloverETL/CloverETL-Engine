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
 * @created 27.1.2013
 */
public class StringLengthValidationRuleTest extends ValidatorTestCase {

	@Test
	public void testNameability() {
		testNameability(StringLengthValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(StringLengthValidationRule.class);
	}
	@Test
	public void testCommon() {
		testCommon(StringLengthValidationRule.class);
	}
	@Test
	public void testReadyness() {
		StringLengthValidationRule temp;
		DataRecord record = RF.addStringField(null, "field", "");
		
		assertReadyness(true, newRule("==", false, "field", 5, 0), record.getMetadata());
		assertReadyness(true, newRule("==", false, "field", 0, 0), record.getMetadata());
		assertReadyness(false, newRule("==", false, "field2", 5, 0), record.getMetadata());
		assertReadyness(false, newRule("==", false, "", 5, 0), record.getMetadata());
		assertReadyness(false, newRule("==", false, "", -5, 0), record.getMetadata());
		temp = newRule("==", false, "", 5, 0);
		temp.getFrom().setValue(null);
		assertReadyness(false, temp, record.getMetadata());
		
		assertReadyness(true, newRule("<=", false, "field", 5, 0), record.getMetadata());
		assertReadyness(true, newRule("<=", false, "field", 0, 0), record.getMetadata());
		assertReadyness(false, newRule("<=", false, "field2", 5, 0), record.getMetadata());
		assertReadyness(false, newRule("<=", false, "", 5, 0), record.getMetadata());
		assertReadyness(false, newRule("<=", false, "field", -5, 0), record.getMetadata());
		temp = newRule("<=", false, "", 5, 0);
		temp.getFrom().setValue(null);
		assertReadyness(false, temp, record.getMetadata());
		
		assertReadyness(true, newRule(">=", false, "field", 0, 5), record.getMetadata());
		assertReadyness(true, newRule(">=", false, "field", 0, 0), record.getMetadata());
		assertReadyness(false, newRule(">=", false, "field2", 0, 5), record.getMetadata());
		assertReadyness(false, newRule(">=", false, "", 0, 5), record.getMetadata());
		assertReadyness(false, newRule(">=", false, "field", 0, -5), record.getMetadata());
		temp = newRule(">=", false, "", 0, 5);
		temp.getTo().setValue(null);
		assertReadyness(false, temp, record.getMetadata());
		
		assertReadyness(true, newRule("<>", false, "field", 0, 5), record.getMetadata());
		assertReadyness(true, newRule("<>", false, "field", 0, 0), record.getMetadata());
		assertReadyness(false, newRule("<>", false, "field2", 0, 5), record.getMetadata());
		assertReadyness(false, newRule("<>", false, "", 0, 5), record.getMetadata());
		assertReadyness(false, newRule("<>", false, "field", 0, -5), record.getMetadata());
		temp = newRule("<>", false, "", 0, 5);
		temp.getTo().setValue(null);
		temp.getFrom().setValue(null);
		assertReadyness(false, temp, record.getMetadata());
	}
	@Test
	public void testNormal() {
		assertInvalid(newRule("==", false, "field", 5, 0), RF.addStringField(null, "field", "1234"));
		assertValid(newRule("==", false, "field", 5, 0), RF.addStringField(null, "field", "12345"));
		assertInvalid(newRule("==", false, "field", 5, 0), RF.addStringField(null, "field", "123456"));
		
		assertInvalid(newRule(">=", false, "field", 6, 0), RF.addStringField(null, "field", "abcd"));
		assertValid(newRule(">=", false, "field", 6, 0), RF.addStringField(null, "field", "abcdef"));
		assertValid(newRule(">=", false, "field", 6, 0), RF.addStringField(null, "field", "abcdefgh"));
		
		assertValid(newRule("<=", false, "field", 0, 6), RF.addStringField(null, "field", "abcd"));
		assertValid(newRule("<=", false, "field", 0, 6), RF.addStringField(null, "field", "abcdef"));
		assertInvalid(newRule("<=", false, "field", 0, 6), RF.addStringField(null, "field", "abcdefgh"));
		
		assertInvalid(newRule("<>", false, "field", 3, 6), RF.addStringField(null, "field", "ab"));
		assertValid(newRule("<>", false, "field", 3, 6), RF.addStringField(null, "field", "abc"));
		assertValid(newRule("<>", false, "field", 3, 6), RF.addStringField(null, "field", "abcd"));
		assertValid(newRule("<>", false, "field", 3, 6), RF.addStringField(null, "field", "abcde"));
		assertValid(newRule("<>", false, "field", 3, 6), RF.addStringField(null, "field", "abcdef"));
		assertInvalid(newRule("<>", false, "field", 3, 6), RF.addStringField(null, "field", "abcdefgh"));
	}
	@Test
	public void testTrimming() {
		assertInvalid(newRule("==", true, "field", 5, 0), RF.addStringField(null, "field", " 1234 "));
		assertValid(newRule("==", true, "field", 5, 0), RF.addStringField(null, "field", "12345		"));
		assertInvalid(newRule("==", true, "field", 5, 0), RF.addStringField(null, "field", "\n123456\n"));
	}
	@Test
	public void testNonStrings() {
		assertValid(newRule("==", true, "field", 5, 0), RF.addLongField(null, "field", 10000l));
		assertInvalid(newRule("==", true, "field", 5, 0), RF.addLongField(null, "field", 100001l));
		
		assertValid(newRule("==", true, "field", 5, 0), RF.addNumberField(null, "field", 112.5));
		assertInvalid(newRule("==", true, "field", 5, 0), RF.addNumberField(null, "field", 112.52));
		
		assertValid(newRule("==", true, "field", 4, 0), RF.addBooleanField(null, "field", true));
		assertInvalid(newRule("==", true, "field", 4, 0), RF.addBooleanField(null, "field", false));
	}
	
	private StringLengthValidationRule newRule(String type, boolean trimming, String target, int left, int right) {
		StringLengthValidationRule rule = createRule(StringLengthValidationRule.class);
		if(type.equals("==")) {
			rule.getType().setValue(StringLengthValidationRule.TYPES.EXACT);
		} else if(type.equals(">=")) {
			rule.getType().setValue(StringLengthValidationRule.TYPES.MINIMAL);
		} else if(type.equals("<=")) {
			rule.getType().setValue(StringLengthValidationRule.TYPES.MAXIMAL);
		} else if(type.equals("<>")) {
			rule.getType().setValue(StringLengthValidationRule.TYPES.INTERVAL);
		} else {
			fail("Cannot match operator to create rule.");
		}
		rule.getFrom().setValue(left);
		rule.getTo().setValue(right);
		rule.getTarget().setValue(target);
		rule.getTrimInput().setValue(trimming);
		return rule;
	}
}
