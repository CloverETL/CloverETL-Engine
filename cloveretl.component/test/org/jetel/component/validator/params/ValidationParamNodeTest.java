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

import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.test.CloverTestCase;
import org.junit.Test;

/**
 * Tests of shared functionality of validation param node: name, tooltip, placeholder, hidden toogle, enabled handler
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.5.2013
 */
public class ValidationParamNodeTest extends CloverTestCase {
	
	@Test 
	public void testNamablity() {
		DummyValidationParamNode temp;
		
		temp = new DummyValidationParamNode();
		assertNull(temp.getName());
		temp.setName("Name");
		assertEquals("Name", temp.getName());
		temp.setName("Name2");
		assertEquals("Name2", temp.getName());
	}
	
	@Test 
	public void testTooltip() {
		DummyValidationParamNode temp;
		
		temp = new DummyValidationParamNode();
		assertNull(temp.getTooltip());
		temp.setTooltip("Tooltip");
		assertEquals("Tooltip", temp.getTooltip());
		temp.setTooltip("Tooltip2");
		assertEquals("Tooltip2", temp.getTooltip());
	}
	
	@Test 
	public void testPlaceholder() {
		DummyValidationParamNode temp;
		
		temp = new DummyValidationParamNode();
		assertNull(temp.getPlaceholder());
		temp.setPlaceholder("Placeholder");
		assertEquals("Placeholder", temp.getPlaceholder());
		temp.setPlaceholder("Placeholder2");
		assertEquals("Placeholder2", temp.getPlaceholder());
	}
	
	@Test 
	public void testHidden() {
		DummyValidationParamNode temp;
		
		temp = new DummyValidationParamNode();
		assertFalse(temp.isHidden());
		temp.setHidden(true);
		assertTrue(temp.isHidden());
		temp.setHidden(false);
		assertFalse(temp.isHidden());
	}
	
	@Test 
	public void testEnabled() {
		DummyValidationParamNode temp;
		
		temp = new DummyValidationParamNode();
		assertTrue(temp.isEnabled());
		temp.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				return false;
			}
		});
		assertFalse(temp.isEnabled());
		temp.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				return true;
			}
		});
		assertTrue(temp.isEnabled());
	}

	private static class DummyValidationParamNode extends ValidationParamNode {
		
	}
}
