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
package org.jetel.component.validator.utils;

import org.jetel.component.validator.CustomRule;
import org.jetel.component.validator.ValidationGroup;
import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.component.validator.rules.ComparisonValidationRule;
import org.jetel.component.validator.rules.DateValidationRule;
import org.jetel.component.validator.rules.EmailValidationRule;
import org.jetel.component.validator.rules.EnumMatchValidationRule;
import org.jetel.component.validator.rules.IntervalValidationRule;
import org.jetel.component.validator.rules.LookupValidationRule;
import org.jetel.component.validator.rules.NonEmptyFieldValidationRule;
import org.jetel.component.validator.rules.NonEmptySubsetValidationRule;
import org.jetel.component.validator.rules.NumberValidationRule;
import org.jetel.component.validator.rules.PatternMatchValidationRule;
import org.jetel.component.validator.rules.StringLengthValidationRule;
import org.junit.Test;

/**
 * Tests basic serialization/deserialization.
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.12.2012
 */
public class ValidationRulesPersisterTest extends ValidatorTestCase {
	
	@Override
	public void setUp() {
		initEngine();
	}
	
	@Test
	public void testSerializationAndDeserialization() {
		ValidationGroup group = new ValidationGroup();
		ValidationGroup subgroup = new ValidationGroup();
		subgroup.addChild(new ComparisonValidationRule());
		subgroup.addChild(new DateValidationRule());
		subgroup.addChild(new EnumMatchValidationRule());
		subgroup.addChild(new IntervalValidationRule());
		subgroup.addChild(new LookupValidationRule());
		subgroup.addChild(new NonEmptyFieldValidationRule());
		subgroup.addChild(new NonEmptySubsetValidationRule());
		subgroup.addChild(new NumberValidationRule());
		subgroup.addChild(new PatternMatchValidationRule());
		subgroup.addChild(new StringLengthValidationRule());
		subgroup.addChild(new EmailValidationRule());
		group.addChild(subgroup);
		try {
			String temp = ValidationRulesPersister.serialize(group);
			ValidationGroup temp2 = ValidationRulesPersister.deserialize(temp);
			String temp3 = ValidationRulesPersister.serialize(temp2);
			assertEquals(temp, temp3);
		} catch (Exception ex) {
			fail("Failed to perform serialization and deserialization");
		}
	}
	
	@Test
	public void testCustomRules() {
		CustomRule first = new CustomRule("First", "Code1");
		CustomRule second = new CustomRule("Second", "Code2");
		ValidationGroup group = new ValidationGroup();
		group.addCustomRule(first);
		group.addCustomRule(second);
		assertSame(first, group.getCustomRule(0));
		assertSame(second, group.getCustomRule(1));
		
		group = ValidationRulesPersister.deserialize(ValidationRulesPersister.serialize(group));
		assertEquals(2, group.getCustomRules().size());
		assertEquals("Code1", group.getCustomRule(0).getCode());
		assertEquals("Code2", group.getCustomRule(1).getCode());
	}
	
	@Test
	public void testGeneratingSchema() {
		try {
			ValidationRulesPersister.generateSchema();
		} catch (Exception ex) {
			fail("Failed to generate schema");
		}
	}
}
