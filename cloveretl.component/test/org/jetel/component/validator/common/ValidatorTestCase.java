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
package org.jetel.component.validator.common;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ValidationNode;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.IntegerValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.test.CloverTestCase;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 26.1.2013
 */
public class ValidatorTestCase extends CloverTestCase {
	
	@Override
	public void setUp() {
		initEngine();
	}
	/* Some common tests */
	protected void testNameability(Class<? extends ValidationNode> testTarget) {
		ValidationNode rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		assertNotNull(rule);
		assertNull(rule.getName());
		String testName1 = "Test name 1";
		String testName2 = "Test name 2";
		rule.setName(testName1);
		assertEquals(testName1, rule.getName());
		rule.setName(testName2);
		assertEquals(testName2, rule.getName());
	}
	
	protected void testDisability(Class<? extends ValidationNode> testTarget) {
		ValidationNode rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		assertFalse(rule.isEnabled());
		assertEquals(State.NOT_VALIDATED, rule.isValid(TestDataRecordFactory.newRecord(), null));
		rule.setEnabled(true);
		assertTrue(rule.isEnabled());
	}
	
	protected void testBooleanAttribute(Class<? extends AbstractValidationRule> testTarget, int key, boolean defaultValue) {
		AbstractValidationRule rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		if(defaultValue) {
			assertTrue(getBooleanParam(rule, key));
		} else {
			assertFalse(getBooleanParam(rule, key));
		}
		setBooleanParam(rule, key, true);
		assertTrue(getBooleanParam(rule, key));
		setBooleanParam(rule, key, false);
		assertFalse(getBooleanParam(rule, key));
	}
	
	protected void testStringAttribute(Class<? extends AbstractValidationRule> testTarget, int key, String defaultValue) {
		AbstractValidationRule rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		assertEquals(defaultValue, getStringParam(rule, key));
		String test1 = "test string 1";
		String test2 = "test string 2";
		setStringParam(rule, key, test1);
		assertEquals(test1, getStringParam(rule, key));
		setStringParam(rule, key, test2);
		assertEquals(test2, getStringParam(rule, key));
		setStringParam(rule, key, null);
		assertNotNull(getStringParam(rule, key));
	}
	
	/* Some setter which handle casting, to clarify the code of tests */
	protected void setBooleanParam(AbstractValidationRule rule, int key, boolean value) {
		((BooleanValidationParamNode) rule.getParam(key)).setValue(value);
	}
	protected void setIntegerParam(AbstractValidationRule rule, int key, int value) {
		((IntegerValidationParamNode) rule.getParam(key)).setValue(Integer.valueOf(value));
	}
	protected void setStringParam(AbstractValidationRule rule, int key, String value) {
		((StringValidationParamNode) rule.getParam(key)).setValue(value);
	}
	protected void setEnumParam(AbstractValidationRule rule, int key, Object value) {
		((EnumValidationParamNode) rule.getParam(key)).setValue(value);
	}
	/* Some getters which handle casting, to clarify the code of tests */
	protected boolean getBooleanParam(AbstractValidationRule rule, int key) {
		return ((BooleanValidationParamNode) rule.getParam(key)).getValue();
	}
	protected Integer getIntegerParam(AbstractValidationRule rule, int key) {
		return ((IntegerValidationParamNode) rule.getParam(key)).getValue();
	}
	protected String getStringParam(AbstractValidationRule rule, int key) {
		return ((StringValidationParamNode) rule.getParam(key)).getValue();
	}
	protected Object getEnumParam(AbstractValidationRule rule, int key) {
		return ((EnumValidationParamNode) rule.getParam(key)).getValue();
	}

}
