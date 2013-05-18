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

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.5.2013
 */
public class BooleanValidationParamNodeTest extends CloverTestCase {
	
	/**
	 * Tests that param node can hold boolean properly and can be created with default value
	 */
	@Test
	public void testSettersAndGetters() {
		BooleanValidationParamNode temp;
		
		temp = new BooleanValidationParamNode(true);
		assertTrue(temp.getValue());
		
		temp = new BooleanValidationParamNode(false);
		assertFalse(temp.getValue());
		
		temp = new BooleanValidationParamNode();
		temp.setValue(true);
		assertTrue(temp.getValue());
		temp.setValue(false);
		assertFalse(temp.getValue());
		
	}
}
