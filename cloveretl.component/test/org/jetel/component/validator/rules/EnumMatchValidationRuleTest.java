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
 * @created 10.12.2012
 */
public class EnumMatchValidationRuleTest extends ValidatorTestCase{
	
	private static final String TARGET = "target";
	private static final String TRIM = "trimInput";
	private static final String VALUES = "values";
	private static final String IGNORE_CASE = "ignoreCase";
	
	@Test
	public void testNameability() {
		testNameability(EnumMatchValidationRule.class);
	}
	
	@Test
	public void testDisablity() {
		testDisability(EnumMatchValidationRule.class);
	}
	@Test
	public void testAttributes() {
		testBooleanAttribute(EnumMatchValidationRule.class, TRIM, false);
		testStringAttribute(EnumMatchValidationRule.class, TARGET, "");
		testStringAttribute(EnumMatchValidationRule.class, VALUES, "");
		testBooleanAttribute(EnumMatchValidationRule.class, IGNORE_CASE, false);
	}
	@Test
	public void testReadyness() {
		AbstractValidationRule rule = new EnumMatchValidationRule();
		assertFalse(rule.isReady());
		setStringParam(rule, TARGET, "some_text");
		assertFalse(rule.isReady());
		setStringParam(rule, VALUES, "");
		assertFalse(rule.isReady());
		setStringParam(rule, VALUES, "one");
		assertTrue(rule.isReady());
		setStringParam(rule, VALUES, "one,two,three");
		assertTrue(rule.isReady());
		setStringParam(rule, TARGET, "");
		assertFalse(rule.isReady());
	}
	
	@Test
	public void testNormal() {		
		AbstractValidationRule rule = new EnumMatchValidationRule();
		rule.setEnabled(true);
		setStringParam(rule, TARGET, "field");
		setStringParam(rule, VALUES,"first,first,second,THIRD,fourth");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "first"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "second"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "THIRD"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "fourth"), null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "five"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "FIRST"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SeCoNd"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Fourth"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "third"), null));
	}
	@Test
	public void testCaseInsensitive() {
		AbstractValidationRule rule = new EnumMatchValidationRule();
		rule.setEnabled(true);
		setStringParam(rule, TARGET, "field");
		setBooleanParam(rule, IGNORE_CASE, true);
		setStringParam(rule, VALUES,"first,first,second,THIRD,fourth");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "first"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "second"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "THIRD"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "fourth"), null));
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "FIRST"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SECOND"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "third"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "FOURTH"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "FiRsT"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Second"), null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "five"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "FIVE"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SiX"), null));
	}
	@Test
	public void testEmptyOption() {
		AbstractValidationRule rule = new EnumMatchValidationRule();
		rule.setEnabled(true);
		setStringParam(rule, TARGET, "field");
		setStringParam(rule, VALUES,",first,second,,third,fourth");
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "first"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "eight"), null));
		
		setStringParam(rule, VALUES,"first,,second,,third,fourth");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "first"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "second"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "eight"), null));
		
		setStringParam(rule, VALUES,"first,second,,third,fourth,");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "fourth"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "eight"), null));
	}
	@Test
	public void testTrimming() {
		AbstractValidationRule rule = new EnumMatchValidationRule();
		rule.setEnabled(true);
		setStringParam(rule, TARGET, "field");
		setBooleanParam(rule, TRIM, true);
		setStringParam(rule, VALUES,"first,second,,THIRD,fourth");
		
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", " first "), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", " first"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "first "), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", " "), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "\n"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "	"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "\nfirst\n"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "\nfirst"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "first\n"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "	first	"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "	first"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "first	"), null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "FiRST"), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "   five   "), null));
	}
	@Test
	public void testEscaping() {
		AbstractValidationRule rule = new EnumMatchValidationRule();
		rule.setEnabled(true);
		setStringParam(rule, TARGET, "field");
		setStringParam(rule, VALUES,"first,second,\"thi,rd\",\"fourth\"");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "thi,rd"), null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "fourth"), null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "\"thi,rd\""), null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "\"fourth\""), null));
		
		setStringParam(rule, VALUES,"\"f,,,irst\",second,\"thi,rd\",\"fourth\",");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "f,,,irst"), null));
		setStringParam(rule, VALUES,",\"f,,,irst\",second,\"thi,rd\",\"fourth\",");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "f,,,irst"), null));
		
		setStringParam(rule, VALUES,"\"\",first,second,\"thi,rd\",\"fourth\"");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		
		setStringParam(rule, VALUES,"first,second,\"\",third,fourth");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ""), null));
		setStringParam(rule, VALUES,"first,second,\",\",third,fourth");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", ","), null));
	}

}
