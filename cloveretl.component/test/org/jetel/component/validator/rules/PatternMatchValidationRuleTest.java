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
public class PatternMatchValidationRuleTest extends ValidatorTestCase {
	@Test
	public void testNameability() {
		testNameability(PatternMatchValidationRule.class);
	}
	
	@Test
	public void testDisablity() {
		testDisability(PatternMatchValidationRule.class);
	}
	@Test
	public void testAttributes() {
		testBooleanAttribute(PatternMatchValidationRule.class, PatternMatchValidationRule.IGNORE_CASE, false);
		testBooleanAttribute(PatternMatchValidationRule.class, PatternMatchValidationRule.TRIM, false);
		testStringAttribute(PatternMatchValidationRule.class, PatternMatchValidationRule.TARGET, "");
		testStringAttribute(PatternMatchValidationRule.class, PatternMatchValidationRule.PATTERN, "");
	}
	@Test
	public void testReadyness() {
		AbstractValidationRule rule = new PatternMatchValidationRule();
		assertFalse(rule.isReady());
		setStringParam(rule, PatternMatchValidationRule.TARGET, "some_text");
		assertFalse(rule.isReady());
		setStringParam(rule, PatternMatchValidationRule.PATTERN , "");
		assertFalse(rule.isReady());
		setStringParam(rule, PatternMatchValidationRule.PATTERN , "pattern");
		assertTrue(rule.isReady());
		setStringParam(rule, NonEmptyFieldValidationRule.TARGET, "");
		assertFalse(rule.isReady());
	}
	@Test
	public void testNormal() {
		AbstractValidationRule rule = new PatternMatchValidationRule();
		rule.setEnabled(true);
		setStringParam(rule, PatternMatchValidationRule.TARGET, "field");
		
		// Simple test that Java regexp really works, no deep testing!
		setStringParam(rule, PatternMatchValidationRule.PATTERN, "^starting$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting some text"), null));
		
		setStringParam(rule, PatternMatchValidationRule.PATTERN, "^(\\d)*$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "9"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1257"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "text"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "9e0"), null));
		
		setStringParam(rule, PatternMatchValidationRule.PATTERN, "^[abc]?$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "a"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "b"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "c"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "e"), null));
		
		setStringParam(rule, PatternMatchValidationRule.PATTERN, "asdfčě+š=é+$$$.\\\\sdf?*-9+?");
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "some text"), null));
	}
	@Test
	public void testCaseInsensitive() {
		AbstractValidationRule rule = new PatternMatchValidationRule();
		rule.setEnabled(true);
		setStringParam(rule, PatternMatchValidationRule.TARGET, "field");
		setBooleanParam(rule, PatternMatchValidationRule.IGNORE_CASE, true);
		
		setStringParam(rule, PatternMatchValidationRule.PATTERN, "^starting$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "STARTING"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "sTarTinG"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting some text"), null));
		
		setStringParam(rule, PatternMatchValidationRule.PATTERN, "^(\\d)*$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "9"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1257"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "text"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "9e0"), null));
		
		setStringParam(rule, PatternMatchValidationRule.PATTERN, "^[abc]?$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "a"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "A"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "b"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "B"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "c"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "C"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "e"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "E"), null));
		
		setStringParam(rule, PatternMatchValidationRule.PATTERN, "asdfčě+š=é+$$$.\\\\sdf?*-9+?");
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "some text"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SOME TEXT"), null));
			
	}
	@Test
	public void testTrimming() {
		AbstractValidationRule rule = new PatternMatchValidationRule();
		rule.setEnabled(true);
		setStringParam(rule, PatternMatchValidationRule.TARGET, "field");
		setBooleanParam(rule, PatternMatchValidationRule.TRIM, true);
		
		setStringParam(rule, PatternMatchValidationRule.PATTERN, "^starting$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", " starting "), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting "), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", " starting"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "\nstarting\n"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "	starting	"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "	starting"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting	"), null));
	}
}
