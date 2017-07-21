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
public class NonEmptySubsetValidationRuleTest extends ValidatorTestCase {
	@Test
	public void testNameability() {
		testNameability(NonEmptySubsetValidationRule.class);
	}
	@Test
	public void testDisability() {
		testDisability(NonEmptySubsetValidationRule.class);
	}
	@Test
	public void testCommon() {
		testCommon(NonEmptySubsetValidationRule.class);
	}
	@Test
	public void testReadyness() {
		DataRecord record = RF.addStringField(null, "field", "");
		record = RF.addStringField(record, "field2", "");
		record = RF.addStringField(record, "field3", "value");
		record = RF.addStringField(record, "field4", "value");
		NonEmptySubsetValidationRule rule;
		
		rule = newRule("field", true, -5, true);
		assertReadyness(false, rule, record.getMetadata());
		
		rule = newRule("field", true, 0, true);
		assertReadyness(false, rule, record.getMetadata());
		
		rule = newRule("field", true, 1, true);
		rule.getCount().setValue(null);
		assertReadyness(false, rule, record.getMetadata());
		
		rule = newRule("field,field2", true, 1, true);
		assertReadyness(true, rule, record.getMetadata());
		
		rule = newRule("field,field5", true, 1, true);
		assertReadyness(false, rule, record.getMetadata());
		
		rule = newRule("", true, 1, true);
		assertReadyness(false, rule, record.getMetadata());
		
	}
	@Test
	public void testEmptiness() {
		DataRecord record = RF.addStringField(null, "field", "");
		record = RF.addStringField(record, "field2", "");
		record = RF.addStringField(record, "field3", "value");
		record = RF.addStringField(record, "field4", "value");
		
		assertValid(newRule("field", true, 0, false), record);
		assertValid(newRule("field", true, 1, false), record);
		assertInvalid(newRule("field", true, 2, false), record);
		
		assertValid(newRule("field,field2,field3,field4", true, 0, false), record);
		assertValid(newRule("field,field2,field3,field4", true, 1, false), record);
		assertValid(newRule("field,field2,field3,field4", true, 2, false), record);
		assertInvalid(newRule("field,field2,field3,field4", true, 3, false), record);
		
		assertValid(newRule("field3,field4", true, 0, false), record);
		assertInvalid(newRule("field3,field4", true, 1, false), record);
		assertInvalid(newRule("field3,field4", true, 2, false), record);
	}
	@Test
	public void testNonEmptiness() {
		DataRecord record = RF.addStringField(null, "field", "value");
		record = RF.addStringField(record, "field2", "value");
		record = RF.addStringField(record, "field3", "value");
		record = RF.addStringField(record, "field4", "value");
				
		assertValid(newRule("field", false, 0, false), record);
		assertValid(newRule("field", false, 1, false), record);
		assertInvalid(newRule("field", false, 2, false), record);
		assertValid(newRule("field,field2,field3", false, 0, false), record);
		assertValid(newRule("field,field2,field3", false, 1, false), record);
		assertValid(newRule("field,field2,field3", false, 2, false), record);
		assertValid(newRule("field,field2,field3", false, 3, false), record);
		assertInvalid(newRule("field,field2,field3", false, 4, false), record);
		
		record = RF.addStringField(null, "field", "value");
		record = RF.addStringField(record, "field2", "");
		record = RF.addStringField(record, "field3", "");
		record = RF.addStringField(record, "field4", "value");
		assertValid(newRule("field,field2,field3,field4", false, 0, false), record);
		assertValid(newRule("field,field2,field3,field4", false, 1, false), record);
		assertInvalid(newRule("field,field2,field3,field4", false, 3, false), record);
	}
	@Test
	public void testTrimming() {
		DataRecord record = RF.addStringField(null, "field", "\n\n\n\r\tvalue");
		record = RF.addStringField(record, "field2", "    	value");
		record = RF.addStringField(record, "field3", " value   ");
		record = RF.addStringField(record, "field4", " value   ");
		record = RF.addStringField(record, "field5", " \n\t\r\n   ");
		assertValid(newRule("field,field2,field3,field4", false, 4, true), record);
		assertValid(newRule("field5", false, 1, false), record);
		
		record = RF.addStringField(null, "field", "\n\n\n\r\t");
		record = RF.addStringField(record, "field2", "    	");
		record = RF.addStringField(record, "field3", "       ");
		record = RF.addStringField(record, "field4", " \n\n   ");
		assertInvalid(newRule("field,field2,field3,field4", true, 4, false), record);
		assertValid(newRule("field,field2,field3,field4", true, 4, true), record);
	}
	
	private NonEmptySubsetValidationRule newRule(String target, boolean checkForEmptiness, int count, boolean trim) {
		NonEmptySubsetValidationRule rule = createRule(NonEmptySubsetValidationRule.class);
		rule.getTarget().setValue(target);
		rule.getGoal().setValue((checkForEmptiness) ? NonEmptySubsetValidationRule.GOALS.EMPTY : NonEmptySubsetValidationRule.GOALS.NONEMPTY);
		rule.getCount().setValue(count);
		rule.getTrimInput().setValue(trim);
		return rule;
	}
}
