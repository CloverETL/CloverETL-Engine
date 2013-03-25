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
	
	@Test
	public void testNameability() {
		testNameability(EnumMatchValidationRule.class);
	}
	
	@Test
	public void testDisablity() {
		testDisability(EnumMatchValidationRule.class);
	}
	@Test
	public void testReadyness() {
		// TODO: 
	}
	
	@Test
	public void testNormal() {		
		EnumMatchValidationRule rule = new EnumMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getValues().setValue("first,first,second,THIRD,fourth");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "first"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "second"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "THIRD"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "fourth"), null,null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "five"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "FIRST"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SeCoNd"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Fourth"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "third"), null,null));
	}
	
	public void testNumbers() {
		EnumMatchValidationRule rule = new EnumMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getValues().setValue("10,20,30,40");
		rule.getUseType().setValue(ConversionValidationRule.METADATA_TYPES.LONG);
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "20"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "30"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "40"), null,null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "15"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "-10"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "21"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "20.5"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "40.0"), null,null));
	}
	
	public void testLocale() {
		EnumMatchValidationRule rule = new EnumMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getValues().setValue("\"10.5\",20,\"30.6\",40");
		rule.getLocale().setValue("cs.CZ");
		rule.getUseType().setValue(ConversionValidationRule.METADATA_TYPES.NUMBER);
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10,5"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "20"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "30,6"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "40"), null,null));
		
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "10.5"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "20"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "30.6"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "40"), null,null));
		
	}
	@Test
	public void testCaseInsensitive() {
		EnumMatchValidationRule rule = new EnumMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getIgnoreCase().setValue(true);
		rule.getValues().setValue("first,first,SeCoND,THIRD,fourth");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "first"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SeCoND"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "THIRD"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "fourth"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "First"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "FIRST"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SeCoNd"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Fourth"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "third"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Five"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "five"), null,null));
	}
	@Test
	public void testEmptyOption() {
	}
	@Test
	public void testTrimming() {
		EnumMatchValidationRule rule = new EnumMatchValidationRule();
		rule.getTarget().setValue("field");
		rule.getTrimInput().setValue(true);
		rule.getValues().setValue("first,first,SeCoND,THIRD,fourth");
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "  first  "), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "SeCoND   "), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "  THIRD"), null,null));
		assertEquals(State.VALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "  fourth  "), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "Five"), null,null));
		assertEquals(State.INVALID, rule.isValid(TestDataRecordFactory.addStringField(null, "field", "five"), null,null));
	}
	@Test
	public void testEscaping() {
		// TODO:
	}

}
