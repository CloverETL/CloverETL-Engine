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
	public void testCommon() {
		testCommon(PatternMatchValidationRule.class);
	}
	@Test
	public void testReadyness() {
		DataRecord record = RF.addStringField(null, "field", "");
		PatternMatchValidationRule rule;
		
		rule = createRule(PatternMatchValidationRule.class);
		assertReadyness(false, rule, record.getMetadata());
		rule.getPattern().setValue("^starting$");
		assertReadyness(false, rule, record.getMetadata());
		rule.getTarget().setValue("field");
		assertReadyness(true, rule, record.getMetadata());
		rule.getTarget().setValue("field2");
		assertReadyness(false, rule, record.getMetadata());
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("");
		assertReadyness(false, rule, record.getMetadata());
	}
	@Test
	public void testNormal() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		
		// Simple test that Java regexp really works, no deep testing!
		rule.getPattern().setValue("^starting$");
		assertValid(rule, RF.addStringField(null, "field", "starting"));
		assertInvalid(rule, RF.addStringField(null, "field", ""));
		assertInvalid(rule, RF.addStringField(null, "field", "starting some text"));
		
		rule.getPattern().setValue("^(\\d)*$");
		assertValid(rule, RF.addStringField(null, "field", ""));
		assertValid(rule, RF.addStringField(null, "field", "9"));
		assertValid(rule, RF.addStringField(null, "field", "1257"));
		assertInvalid(rule, RF.addStringField(null, "field", "text"));
		assertInvalid(rule, RF.addStringField(null, "field", "9e0"));
		
		rule.getPattern().setValue("^[abc]?$");
		assertValid(rule, RF.addStringField(null, "field", ""));
		assertValid(rule, RF.addStringField(null, "field", "a"));
		assertValid(rule, RF.addStringField(null, "field", "b"));
		assertValid(rule, RF.addStringField(null, "field", "c"));
		assertInvalid(rule, RF.addStringField(null, "field", "e"));
		
		rule.getPattern().setValue("asdfčě+š=é+$$$.\\\\sdf?*-9+?");
		assertInvalid(rule, RF.addStringField(null, "field", ""));
		assertInvalid(rule, RF.addStringField(null, "field", "some text"));
	}
	@Test
	public void testCaseInsensitive() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getIgnoreCase().setValue(true);
		
		rule.getPattern().setValue("^starting$");
		assertValid(rule, RF.addStringField(null, "field", "starting"));
		assertValid(rule, RF.addStringField(null, "field", "STARTING"));
		assertValid(rule, RF.addStringField(null, "field", "sTarTinG"));
		assertInvalid(rule, RF.addStringField(null, "field", ""));
		assertInvalid(rule, RF.addStringField(null, "field", "starting some text"));
		
		rule.getPattern().setValue("^(\\d)*$");
		assertValid(rule, RF.addStringField(null, "field", ""));
		assertValid(rule, RF.addStringField(null, "field", "9"));
		assertValid(rule, RF.addStringField(null, "field", "1257"));
		assertInvalid(rule, RF.addStringField(null, "field", "text"));
		assertInvalid(rule, RF.addStringField(null, "field", "9e0"));
		
		rule.getPattern().setValue("^[abc]?$");
		assertValid(rule, RF.addStringField(null, "field", ""));
		assertValid(rule, RF.addStringField(null, "field", "a"));
		assertValid(rule, RF.addStringField(null, "field", "A"));
		assertValid(rule, RF.addStringField(null, "field", "b"));
		assertValid(rule, RF.addStringField(null, "field", "B"));
		assertValid(rule, RF.addStringField(null, "field", "c"));
		assertValid(rule, RF.addStringField(null, "field", "C"));
		assertInvalid(rule, RF.addStringField(null, "field", "e"));
		assertInvalid(rule, RF.addStringField(null, "field", "E"));
		
		rule.getPattern().setValue("asdfčě+š=é+$$$.\\\\sdf?*-9+?");
		assertInvalid(rule, RF.addStringField(null, "field", ""));
		assertInvalid(rule, RF.addStringField(null, "field", "some text"));
		assertInvalid(rule, RF.addStringField(null, "field", "SOME TEXT"));
			
	}
	@Test
	public void testTrimming() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getTrimInput().setValue(true);
		
		rule.getPattern().setValue("^starting$");
		assertValid(rule, RF.addStringField(null, "field", "starting"));
		assertValid(rule, RF.addStringField(null, "field", " starting "));
		assertValid(rule, RF.addStringField(null, "field", "starting "));
		assertValid(rule, RF.addStringField(null, "field", " starting"));
		assertValid(rule, RF.addStringField(null, "field", "\nstarting\n"));
		assertValid(rule, RF.addStringField(null, "field", "	starting	"));
		assertValid(rule, RF.addStringField(null, "field", "	starting"));
		assertValid(rule, RF.addStringField(null, "field", "starting	"));
	}
	
	public void testBooleanInput() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		
		rule.getPattern().setValue("^True$");
		assertValid(rule, RF.addBooleanField(null, "field", true));
		assertInvalid(rule, RF.addBooleanField(null, "field", false));
		
		rule.getPattern().setValue("^False$");
		assertInvalid(rule, RF.addBooleanField(null, "field", true));
		assertValid(rule, RF.addBooleanField(null, "field", false));
	}
	
	public void testDateInput() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		
		rule.getPattern().setValue("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");
		assertValid(rule, RF.addDateField(null, "field", getDate("2013-03-23 10:00:00","UTC")));
		assertInvalid(rule, RF.addStringField(null, "field", "Some value"));
	}
	
	public void testDateInputInCustomFormat() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^\\d{4}-\\d{2}-\\d{2}$");
		rule.getLanguageSettings(0).getDateFormat().setValue("yyyy-MM-dd");
		
		assertValid(rule, RF.addDateField(null, "field", getDate("2013-03-23 10:00:00","UTC")));
		assertInvalid(rule, RF.addStringField(null, "field", "Some value"));
	}
	
	public void testNumberInput() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}\\.?\\d{0,}$");
		
		assertValid(rule, RF.addNumberField(null, "field", 0d));
		assertValid(rule, RF.addNumberField(null, "field", 10.2));
		assertValid(rule, RF.addNumberField(null, "field", -28.333));
		assertValid(rule, RF.addNumberField(null, "field", 1.15));
		assertInvalid(rule, RF.addStringField(null, "field", "Some value"));
		assertInvalid(rule, RF.addStringField(null, "field", "10,5"));
		assertValid(rule, RF.addStringField(null, "field", "10.5"));
	}
	
	public void testDecimalInput() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}\\.?\\d{0,}$");
		
		assertValid(rule, RF.addDecimalField(null, "field", getDecimal("0")));
		assertValid(rule, RF.addDecimalField(null, "field", getDecimal("10.2")));
		assertValid(rule, RF.addDecimalField(null, "field", getDecimal("-28.333")));
		assertValid(rule, RF.addDecimalField(null, "field", getDecimal("1.15")));
		assertInvalid(rule, RF.addStringField(null, "field", "Some value"));
		assertInvalid(rule, RF.addStringField(null, "field", "10,5"));
		assertValid(rule, RF.addStringField(null, "field", "10.5"));
	}
	
	public void testIntegerAndLongInput() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}");
		
		assertValid(rule, RF.addIntegerField(null, "field", 123));
		assertValid(rule, RF.addLongField(null, "field", 10l));
		assertValid(rule, RF.addLongField(null, "field", 0l));
		assertValid(rule, RF.addLongField(null, "field", -185898l));
	}
	
	public void testNumberInputInLocale() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}\\,?\\d{0,}$");
		rule.getLanguageSettings(0).getLocale().setValue("cs.CZ");
		
		assertValid(rule, RF.addNumberField(null, "field", 0d));
		assertValid(rule, RF.addNumberField(null, "field", 10.2));
		assertValid(rule, RF.addNumberField(null, "field", -28.333));
		assertValid(rule, RF.addNumberField(null, "field", 1.15));
		assertInvalid(rule, RF.addStringField(null, "field", "Some value"));
		assertValid(rule, RF.addStringField(null, "field", "10,5"));
		assertInvalid(rule, RF.addStringField(null, "field", "10.5"));
	}
	
	public void testNumberInputInCustomFormat() {
		PatternMatchValidationRule rule = createRule(PatternMatchValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("^[-]?\\d{1,}\\,?\\d? Kč$");
		rule.getLanguageSettings(0).getNumberFormat().setValue("#.# 'Kč'");
		rule.getLanguageSettings(0).getLocale().setValue("cs.CZ");
		
		assertValid(rule, RF.addNumberField(null, "field", 0d));
		assertValid(rule, RF.addNumberField(null, "field", 10.2));
		assertValid(rule, RF.addNumberField(null, "field", -28.333));
		assertValid(rule, RF.addNumberField(null, "field", 1.15));
		assertInvalid(rule, RF.addStringField(null, "field", "Some value"));
		assertInvalid(rule, RF.addStringField(null, "field", "10,5"));
		assertValid(rule, RF.addStringField(null, "field", "10,5 Kč"));
	}	
	
}
