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
	public void testReadyness() {
		// TODO:
	}
	@Test
	public void testNormal() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		
		// Simple test that Java regexp really works, no deep testing!
		rule.getPattern().setValue("^starting$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting some text"), null, null));
		
		rule.getPattern().setValue("^(\\d)*$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "9"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1257"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "text"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "9e0"), null, null));
		
		rule.getPattern().setValue("^[abc]?$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "a"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "b"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "c"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "e"), null, null));
		
		rule.getPattern().setValue("asdfčě+š=é+$$$.\\\\sdf?*-9+?");
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "some text"), null, null));
	}
	@Test
	public void testCaseInsensitive() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getIgnoreCase().setValue(true);
		
		rule.getPattern().setValue("^starting$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "STARTING"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "sTarTinG"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting some text"), null, null));
		
		rule.getPattern().setValue("^(\\d)*$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "9"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "1257"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "text"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "9e0"), null, null));
		
		rule.getPattern().setValue("^[abc]?$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "a"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "A"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "b"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "B"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "c"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "C"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "e"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "E"), null, null));
		
		rule.getPattern().setValue("asdfčě+š=é+$$$.\\\\sdf?*-9+?");
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "some text"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SOME TEXT"), null, null));
			
	}
	@Test
	public void testTrimming() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getTrimInput().setValue(true);
		
		rule.getPattern().setValue("^starting$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", " starting "), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting "), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", " starting"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "\nstarting\n"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "	starting	"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "	starting"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "starting	"), null, null));
	}
	
	public void testBooleanInput() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		
		rule.getPattern().setValue("^True$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "field", true), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "field", false), null, null));
		
		rule.getPattern().setValue("^False$");
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "field", true), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addBooleanField(null, "field", false), null, null));
	}
	
	public void testDateInput() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		
		rule.getPattern().setValue("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDateField(null, "field", getDate("2013-03-23 10:00:00","UTC")), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Some value"), null, null));
	}
	
	public void testDateInputInCustomFormat() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^\\d{4}-\\d{2}-\\d{2}$");
		rule.getLanguageSettings(0).getDateFormat().setValue("yyyy-MM-dd");
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDateField(null, "field", getDate("2013-03-23 10:00:00","UTC")), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Some value"), null, null));
	}
	
	public void testNumberInput() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}\\.?\\d{0,}$");
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", 0d), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", 10.2), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", -28.333), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", 1.15), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Some value"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10,5"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10.5"), null, null));
	}
	
	public void testDecimalInput() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}\\.?\\d{0,}$");
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("0")), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("10.2")), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("-28.333")), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addDecimalField(null, "field", getDecimal("1.15")), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Some value"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10,5"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10.5"), null, null));
	}
	
	public void testIntegerAndLongInput() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}");
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addIntegerField(null, "field", 123), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addLongField(null, "field", 10l), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addLongField(null, "field", 0l), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addLongField(null, "field", -185898l), null, null));
	}
	
	public void testNumberInputInLocale() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}\\,?\\d{0,}$");
		// TODO: fixme (without format english style is used)
		rule.getLanguageSettings(0).getLocale().setValue("cs.CZ");
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", 0d), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", 10.2), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", -28.333), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", 1.15), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Some value"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10,5"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10.5"), null, null));
	}
	
	public void testNumberInputInCustomFormat() {
		PatternMatchValidationRule rule = new PatternMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}\\,?\\d? Kč$");
		rule.getLanguageSettings(0).getNumberFormat().setValue("#,# 'Kč'");
		rule.getLanguageSettings(0).getLocale().setValue("cs.CZ");
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", 0d), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", 10.2), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", -28.333), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addNumberField(null, "field", 1.15), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Some value"), null, null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10,5"), null, null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10,5 Kč"), null, null));
	}	
	
}
