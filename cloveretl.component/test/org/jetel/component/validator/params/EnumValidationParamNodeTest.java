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
package org.jetel.component.validator.params;

import org.jetel.test.CloverTestCase;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.5.2013
 */
public class EnumValidationParamNodeTest extends CloverTestCase {
	
	private enum TEST1 {
		A,B,C,D
	}
	private enum TEST2 {
		A,B,C,D,E
	}
	
	/**
	 * Tests that param node:
	 *  - can be set properly
	 *  - can hold only enum values
	 */
	@Test
	public void testSettersAndGetters() {
		EnumValidationParamNode temp;
		
		temp = new EnumValidationParamNode(TEST1.values(), null);
		assertNull(temp.getValue());
		assertArrayEquals(temp.getOptions(), TEST1.values());
		
		temp = new EnumValidationParamNode(TEST1.values(), TEST2.A);
		assertNull(temp.getValue());
		
		temp = new EnumValidationParamNode(TEST1.values(), TEST1.B);
		assertEquals(TEST1.B, temp.getValue());
		assertNotSame(TEST1.A, temp.getValue());
		assertNotSame(TEST2.A, temp.getValue());
		
		temp = new EnumValidationParamNode(TEST1.values(), TEST1.D);
		assertEquals(TEST1.D, temp.getValue());
		temp.setValue(TEST2.B);
		assertEquals(TEST1.D, temp.getValue());
		temp.setValue(TEST1.C);
		assertEquals(TEST1.C, temp.getValue());
		temp.setValue(null);
		assertEquals(TEST1.C, temp.getValue());
	}
	
	/**
	 * Tests that param node:
	 * 	- can be created from string values
	 */
	@Test
	public void testFromString() {
		EnumValidationParamNode temp;
		
		temp = new EnumValidationParamNode(TEST1.values(), null);
		
		temp.setFromString("Ahoj");
		assertNull(temp.getValue());
		
		temp.setFromString(TEST2.E.name());
		assertNull(temp.getValue());
		
		temp.setFromString(TEST1.D.name());
		assertEquals(TEST1.D, temp.getValue());
		
		temp.setFromString(TEST2.E.name());
		assertEquals(TEST1.D, temp.getValue());
		
		temp.setFromString("G");
		assertEquals(TEST1.D, temp.getValue());
	}
}
