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

import junit.framework.Assert;

import org.jetel.component.validator.CustomRule;
import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.5.2013
 */
public class CustomValidationRuleTest extends ValidatorTestCase {
	
	private CustomRule oddUnaryCustomRule = new CustomRule("OddUnary", "//#CTL2\n" +
			"function void oddUnary(integer input) { $out.0.result = (input % 2) == 1; }");
	
	private CustomRule oddBinaryCustomRule = new CustomRule("OddBinary", "//#CTL2\n" +
			"function void oddBinary(integer input, integer input2) { $out.0.result = (input % 2) == 1 && (input2 % 2) == 1;}");
	
	private CustomRule wrongCode = new CustomRule("WrongCode", "//#CTL2\n" +
			"function void wrong(integer input) { asdfasdfsadfsadfsadfsdafsdaf }");
	
	private CustomRule noFunction = new CustomRule("NoFunction", "//#CTL2\n");
	@Test
	public void testNameability() {
		testNameability(CustomValidationRule.class);
	}
	
	@Test
	public void testDisability() {
		testDisability(CustomValidationRule.class);
	}
	@Test
	public void testCommon() {
		testCommon(CustomValidationRule.class);
	}
	
	@Test
	public void testNonExistent() {
		DataRecord record = RF.addStringField(null, "field", null);

		CustomValidationRule temp;
		temp = createRule(CustomValidationRule.class);
		assertReadyness(false, temp, record.getMetadata());
		
		temp = createRule(CustomValidationRule.class);
		temp.getRef().setValue(10);
		assertReadyness(false, temp, record.getMetadata());
	}
	
	@Test
	public void testUncompilable() {
		DataRecord record = RF.addStringField(null, "field", null);
		
		DummyGraphWrapper graphWrapper = new DummyGraphWrapper();
		graphWrapper.addCustomRule(1, wrongCode);
		graphWrapper.addCustomRule(2, noFunction);
		
		CustomValidationRule temp;
		temp = createRule(CustomValidationRule.class);
		temp.getRef().setValue(2);
		assertReadyness(false, temp, record.getMetadata(), null, graphWrapper);
		
		temp = createRule(CustomValidationRule.class);
		temp.getRef().setValue(1);
		assertReadyness(false, temp, record.getMetadata(), null, graphWrapper);
	}
	
	@Test
	public void testWrongArguments() {
		DataRecord record = RF.addStringField(RF.addIntegerField((RF.addIntegerField(null, "field3", 12)), "field2", 10), "field", null);
		
		DummyGraphWrapper graphWrapper = new DummyGraphWrapper();
		graphWrapper.addCustomRule(1, oddUnaryCustomRule);
		graphWrapper.addCustomRule(2, oddBinaryCustomRule);
		
		CustomValidationRule temp;
		temp = createRule(CustomValidationRule.class);
		temp.getRef().setValue(1);
		temp.getTarget().setValue("field");
		assertReadyness(false, temp, record.getMetadata(), null, graphWrapper);
		
		temp = createRule(CustomValidationRule.class);
		temp.getRef().setValue(1);
		temp.getTarget().setValue("zzzz");
		temp.getMappingParam().setValue("input:=field2");
		assertReadyness(true, temp, record.getMetadata(), null, graphWrapper);
		
		temp = createRule(CustomValidationRule.class);
		temp.getRef().setValue(2);
		temp.getTarget().setValue("zzzz");
		temp.getMappingParam().setValue("input:=field;input2:=field2");
		assertReadyness(false, temp, record.getMetadata(), null, graphWrapper);
		
		temp = createRule(CustomValidationRule.class);
		temp.getRef().setValue(2);
		temp.getTarget().setValue("zzzz");
		temp.getMappingParam().setValue("input:=field3;input2:=field2");
		assertReadyness(true, temp, record.getMetadata(), null, graphWrapper);
	}
	
	@Test
	public void testCorrect() {
		DataRecord record1 = RF.addIntegerField(null, "field", 10);
		DataRecord record2 = RF.addIntegerField(null, "field", 9);
		record2 = RF.addIntegerField(record2, "field2", 6);
		DataRecord record3 = RF.addIntegerField(null, "field", 9);
		record3 = RF.addIntegerField(record3, "field2", 7);
		
		DummyGraphWrapper graphWrapper = new DummyGraphWrapper();
		graphWrapper.addCustomRule(1, oddUnaryCustomRule);
		graphWrapper.addCustomRule(2, oddBinaryCustomRule);
		
		CustomValidationRule temp;
		temp = createRule(CustomValidationRule.class);
		temp.getRef().setValue(1);
		temp.getTarget().setValue("zzz");
		temp.getMappingParam().setValue("input:=field");
		try {
			temp.init(record1, graphWrapper);
		} catch (ComponentNotReadyException e) {
			Assert.fail(e.getMessage());
		}
		assertInvalid(temp, record1, null, graphWrapper);
		try {
			temp.init(record2, graphWrapper);
		} catch (ComponentNotReadyException e) {
			Assert.fail(e.getMessage());
		}
		assertValid(temp, record2, null, graphWrapper);
		
		temp = createRule(CustomValidationRule.class);
		temp.getRef().setValue(2);
		temp.getTarget().setValue("zzz");
		temp.getMappingParam().setValue("input:=field;input2:=field2");
		
		try {
			temp.init(record3, graphWrapper);
		} catch (ComponentNotReadyException e) {
			Assert.fail(e.getMessage());
		}
		assertValid(temp, record3, null, graphWrapper);
		
		try {
			temp.init(record2, graphWrapper);
		} catch (ComponentNotReadyException e) {
			Assert.fail(e.getMessage());
		}
		assertInvalid(temp, record2, null, graphWrapper);
		
		
	}
}
