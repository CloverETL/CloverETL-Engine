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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.data.DataRecord;
import org.junit.Test;

/**
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30.5.2013
 */
public class EmailValidationRuleTest extends ValidatorTestCase {
	@Test
	public void testNameability() {
		testNameability(EmailValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(EmailValidationRule.class);
	}
	@Test
	public void testCommon() {
		testCommon(EmailValidationRule.class);
	}
	@Test
	public void testReadiness() {
		DataRecord record = RF.addStringField(null, "field", "");
		record = RF.addStringField(record, "field2", "");
		record = RF.addStringField(record, "field3", "value");
		record = RF.addStringField(record, "field4", "value");
		EmailValidationRule rule;
		
		rule = newRule("field5", false, false);
		assertReadyness(false, rule, record.getMetadata());
		
		record = RF.addIntegerField(record, "field5", 1);
		rule = newRule("field5", false, false);
		assertReadyness(false, rule, record.getMetadata());
		
		record = RF.addStringField(record, "field6", "");
		rule = newRule("field6", false, false);
		assertReadyness(true, rule, record.getMetadata());
	}
	
	@Test
	public void testEmailValidation() throws IOException {
		InputStream emailsStream = getClass().getResourceAsStream("emails.txt");
		BufferedReader bis = new BufferedReader(new InputStreamReader(emailsStream));
		DataRecord record = RF.addStringField(null, "field", "");
		
		EmailValidationRule liberalRule = newRule("field", false, true);
		liberalRule.setName("Liberal rule");
		EmailValidationRule strictRule = newRule("field", true, false);
		strictRule.setName("Strict rule");
		EmailValidationRule plainRule = newRule("field", true, true);
		plainRule.setName("Plain rule");
		EmailValidationRule noGroupRule = newRule("field", false, false);
		noGroupRule.setName("No group rule");
		
		for (String line = bis.readLine(); line != null; line = bis.readLine()) {
			if (line.startsWith(";") || line.isEmpty()) {
				continue;
			}
			record.getField(0).setValue(line.substring(4));
			int validFlag = line.charAt(0);
			int plainFlag = line.charAt(1);
			int groupFlag = line.charAt(2);
			
			if (validFlag == 'V') {
				assertValid(liberalRule, record);
				if (plainFlag == 'P' && groupFlag == 'g') {
					assertValid(strictRule, record);
					assertValid(noGroupRule, record);
					assertValid(plainRule, record);
				}
				else if (plainFlag == 'P' && groupFlag == 'G') {
					assertInvalid(strictRule, record);
					assertInvalid(noGroupRule, record);
					assertValid(plainRule, record);
				}
				else if (plainFlag == 'p' && groupFlag == 'G') {
					assertInvalid(strictRule, record);
					assertInvalid(noGroupRule, record);
					assertInvalid(plainRule, record);
				}
				else if (plainFlag == 'p' && groupFlag == 'g') {
					assertInvalid(strictRule, record);
					assertValid(noGroupRule, record);
					assertInvalid(plainRule, record);
				}
			}
			else if (validFlag == 'I') {
				assertInvalid(liberalRule, record);
				assertInvalid(strictRule, record);
				assertInvalid(plainRule, record);
				assertInvalid(noGroupRule, record);
			}
		}
	}
	
	private EmailValidationRule newRule(String target, boolean plainAddress, boolean allowGroupAddresses) {
		EmailValidationRule rule = createRule(EmailValidationRule.class);
		rule.getTarget().setValue(target);
		rule.getAllowGroupAddressesParam().setValue(allowGroupAddresses);
		rule.getPlainAddressParam().setValue(plainAddress);
		return rule;
	}


	

}
