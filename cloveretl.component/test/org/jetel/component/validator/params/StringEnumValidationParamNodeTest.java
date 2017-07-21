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

import static org.junit.Assert.assertArrayEquals;

import org.jetel.test.CloverTestCase;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.5.2013
 */
public class StringEnumValidationParamNodeTest extends CloverTestCase {
	
	private String[] TEST = {"A","B","C","D"};
	
	/**
	 * Tests that param node can hold string properly and can be created with default value and options
	 */
	@Test
	public void testSettersAndGetters() {
		StringEnumValidationParamNode temp;
		
		temp = new StringEnumValidationParamNode();
		assertNotNull(temp.getValue());
		
		temp = new StringEnumValidationParamNode("default");
		assertEquals("default", temp.getValue());
		temp.setValue("new");
		assertEquals("new", temp.getValue());
		temp.setValue(null);
		assertNotNull(temp.getValue());
		
		temp = new StringEnumValidationParamNode();
		assertNull(temp.getOptions());
		temp.setOptions(TEST);
		assertArrayEquals(temp.getOptions(), TEST);
	}
}
