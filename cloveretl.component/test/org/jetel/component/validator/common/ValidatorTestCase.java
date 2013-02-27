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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ValidationNode;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.IntegerValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.data.GetVal;
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
	
	protected void testBooleanAttribute(Class<? extends AbstractValidationRule> testTarget, String attributeName, boolean defaultValue) {
		AbstractValidationRule rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		if(defaultValue) {
			assertTrue(getBooleanParam(rule, attributeName));
		} else {
			assertFalse(getBooleanParam(rule, attributeName));
		}
		setBooleanParam(rule, attributeName, true);
		assertTrue(getBooleanParam(rule, attributeName));
		setBooleanParam(rule, attributeName, false);
		assertFalse(getBooleanParam(rule, attributeName));
	}
	
	protected void testStringAttribute(Class<? extends AbstractValidationRule> testTarget, String attributeName, String defaultValue) {
		AbstractValidationRule rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		assertEquals(defaultValue, getStringParam(rule, attributeName));
		String test1 = "test string 1";
		String test2 = "test string 2";
		setStringParam(rule, attributeName, test1);
		assertEquals(test1, getStringParam(rule, attributeName));
		setStringParam(rule, attributeName, test2);
		assertEquals(test2, getStringParam(rule, attributeName));
		setStringParam(rule, attributeName, null);
		assertNotNull(getStringParam(rule, attributeName));
	}
	
	protected void testIntegerAttribute(Class<? extends AbstractValidationRule> testTarget, String attributeName, Integer defaultValue) {
		AbstractValidationRule rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		assertEquals(defaultValue, getIntegerParam(rule, attributeName));
		Integer test1 = Integer.valueOf(10);
		Integer test2 = Integer.valueOf(-9);
		setIntegerParam(rule, attributeName, test1);
		assertEquals(test1, getIntegerParam(rule, attributeName));
		setIntegerParam(rule, attributeName, test2);
		assertEquals(test2, getIntegerParam(rule, attributeName));
		setIntegerParam(rule, attributeName, null);
		assertNull(getIntegerParam(rule, attributeName));
	}
	
	private static enum TestEnum {
		ONE, TWO, THREE;
	}
	protected void testEnumAttribute(Class<? extends AbstractValidationRule> testTarget, String attributeName, Enum[] options, Object defaultOption) {
		AbstractValidationRule rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		assertEquals(defaultOption, getEnumParam(rule, attributeName));
		for(Enum curr : options) {
			setEnumParam(rule, attributeName, curr);
			assertEquals(curr, getEnumParam(rule, attributeName));
		}
		setEnumParam(rule, attributeName, null);
		assertNotNull(getEnumParam(rule, attributeName));
		setEnumParam(rule, attributeName, TestEnum.ONE);
		assertEquals(options[options.length-1], getEnumParam(rule, attributeName));
	}
	
	/* Some setter which handle casting, to clarify the code of tests */
	protected void setBooleanParam(AbstractValidationRule rule, String attributeName, boolean value) {
		BooleanValidationParamNode temp = (BooleanValidationParamNode) invokeGetter(rule, attributeName); 
		if(temp != null) {
			temp.setValue(value);
		}
	}
	protected void setIntegerParam(AbstractValidationRule rule, String attributeName, int value) {
		IntegerValidationParamNode temp = (IntegerValidationParamNode) invokeGetter(rule, attributeName); 
		if(temp != null) {
			temp.setValue(value);
		}
	}
	protected void setIntegerParam(AbstractValidationRule rule, String attributeName, Integer value) {
		IntegerValidationParamNode temp = (IntegerValidationParamNode) invokeGetter(rule, attributeName); 
		if(temp != null) {
			temp.setValue(value);
		}
	}
	protected void setStringParam(AbstractValidationRule rule, String attributeName, String value) {
		StringValidationParamNode temp = (StringValidationParamNode) invokeGetter(rule, attributeName); 
		if(temp != null) {
			temp.setValue(value);
		}
	}
	protected void setEnumParam(AbstractValidationRule rule, String attributeName, Enum value) {
		EnumValidationParamNode temp = (EnumValidationParamNode) invokeGetter(rule, attributeName); 
		if(temp != null) {
			temp.setValue(value);
		}
	}
	/* Some getters which handle casting, to clarify the code of tests */
	protected boolean getBooleanParam(AbstractValidationRule rule, String attributeName) {
		BooleanValidationParamNode temp = (BooleanValidationParamNode) invokeGetter(rule, attributeName); 
		if(temp == null) {
			fail("Getter returned null, something is wrong with the rule's getter!");
		}
		return temp.getValue();
	}
	protected Integer getIntegerParam(AbstractValidationRule rule, String attributeName) {
		IntegerValidationParamNode temp = (IntegerValidationParamNode) invokeGetter(rule, attributeName); 
		if(temp == null) {
			fail("Getter returned null, something is wrong with the rule's getter!");
		}
		return temp.getValue();
	}
	protected String getStringParam(AbstractValidationRule rule, String attributeName) {
		StringValidationParamNode temp = (StringValidationParamNode) invokeGetter(rule, attributeName); 
		if(temp == null) {
			fail("Getter returned null, something is wrong with the rule's getter!");
		}
		return temp.getValue();
	}
	protected Enum getEnumParam(AbstractValidationRule rule, String attributeName) {
		EnumValidationParamNode temp = (EnumValidationParamNode) invokeGetter(rule, attributeName); 
		if(temp == null) {
			fail("Getter returned null, something is wrong with the rule's getter!");
		}
		return temp.getValue();
	}
	
	private Object invokeGetter(AbstractValidationRule rule, String attributeName) {
		Class<?> testTarget = rule.getClass();
		try {
			Method getter = testTarget.getMethod("get" + firstCharToUpper(attributeName), new Class[0]);
			return getter.invoke(rule, new Object[0]);
		} catch (SecurityException e) {
			fail("Wrong test, getters for param nodes should be public.");
		} catch (NoSuchMethodException e) {
			fail("Wrong test, getters for param nodes should exist.");
		} catch (IllegalArgumentException e) {
			fail("Wrong test, getters for param nodes should be public.");
		} catch (IllegalAccessException e) {
			fail("Wrong instance given, no such method.");
		} catch (InvocationTargetException e) {
			fail("Getter has thrown an exception.");
		}
		return null;
	}
	
	private String firstCharToUpper(String input) {
		return Character.toUpperCase(input.charAt(0)) + input.substring(1);
	}

}
