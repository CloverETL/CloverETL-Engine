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
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3.6.2013
 */
public class PhoneNumberRuleTest extends ValidatorTestCase {
	@Test
	public void testNameability() {
		testNameability(PhoneNumberValidationRule.class);
	}
	@Test
	public void testDisablity() {
		testDisability(PhoneNumberValidationRule.class);
	}
	@Test
	public void testCommon() {
		testCommon(PhoneNumberValidationRule.class);
	}
	
	@Test
	public void testReadyness() {
		DataRecord record = RF.addStringField(null, "field", "");
		PhoneNumberValidationRule rule;
		
		rule = createRule(PhoneNumberValidationRule.class);
		assertReadyness(false, rule, record.getMetadata());
		
		rule.getTarget().setValue("field1");
		assertReadyness(false, rule, record.getMetadata());
		
		rule.getTarget().setValue("field");
		assertReadyness(true, rule, record.getMetadata());
		
		rule.getRegion().setValue("XX");
		assertReadyness(false, rule, record.getMetadata());
		
		rule.getRegion().setValue("US");
		assertReadyness(true, rule, record.getMetadata());
		
		rule.getPattern().setValue("{{{");
		assertReadyness(false, rule, record.getMetadata());
		
		rule.getPattern().setValue("DD10 DD");
		assertReadyness(true, rule, record.getMetadata());
	}
	
	@Test
	public void testValidPhoneNumber() {
		DataRecord record = RF.addStringField(null, "field", "");
		PhoneNumberValidationRule rule;
		rule = createRule(PhoneNumberValidationRule.class);
		rule.getTarget().setValue("field");
		
		record.getField(0).setValue("FBUFBIU");
		assertInvalid(rule, record);
		
		record.getField(0).setValue("+420777666555");
		assertValid(rule, record);
		
		record.getField(0).setValue("777666555");
		rule = createRule(PhoneNumberValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getRegion().setValue("CZ");
		assertValid(rule, record);
		
		record.getField(0).setValue("777666555");
		rule = createRule(PhoneNumberValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getRegion().setValue("US");
		assertInvalid(rule, record);
	}

	@Test
	public void testPhoneNumberFormat() {
		DataRecord record = RF.addStringField(null, "field", "");
		PhoneNumberValidationRule rule;
		rule = createRule(PhoneNumberValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("+420 DDD DDD DDD");
		rule.getTrimInput().setValue(false);
		
		record.getField(0).setValue("+420 777 666 555");
		assertValid(rule, record);
		
		record.getField(0).setValue("+420777 666 555");
		assertInvalid(rule, record);
		
		record.getField(0).setValue("+421 777 666 555");
		assertInvalid(rule, record);
		
		record.getField(0).setValue(" +420 777 666 555 ");
		assertInvalid(rule, record);
		
		// ---
		
		rule = createRule(PhoneNumberValidationRule.class);
		rule.getTarget().setValue("field");
		rule.getPattern().setValue("+420 DDD DDD DDD");
		rule.getTrimInput().setValue(true);
		
		record.getField(0).setValue(" +420 777 666 555 ");
		assertValid(rule, record);
	}
	
}
