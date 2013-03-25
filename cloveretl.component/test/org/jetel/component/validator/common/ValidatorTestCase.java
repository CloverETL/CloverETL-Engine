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

import org.jetel.component.validator.ValidationNode;
import org.jetel.component.validator.ValidationNode.State;
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
		assertEquals(rule.getName(), rule.getCommonName());
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
		assertTrue(rule.isEnabled());
		rule.setEnabled(false);
		assertFalse(rule.isEnabled());
		assertEquals(State.NOT_VALIDATED, rule.isValid(TestDataRecordFactory.newRecord(), null, null));
		assertFalse(rule.isEnabled());
	}
}
