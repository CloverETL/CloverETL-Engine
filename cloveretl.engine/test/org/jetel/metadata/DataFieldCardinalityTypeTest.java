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
package org.jetel.metadata;

import java.util.Arrays;
import java.util.List;

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17 Jan 2012
 */
public class DataFieldCardinalityTypeTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	public void testFromString() {
		assertEquals(DataFieldCardinalityType.SINGLE, DataFieldCardinalityType.fromString("siNglE"));
		assertEquals(DataFieldCardinalityType.LIST, DataFieldCardinalityType.fromString("list"));
		assertEquals(DataFieldCardinalityType.MAP, DataFieldCardinalityType.fromString("MAP"));
		
		try { DataFieldCardinalityType.fromString(null); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
		try { DataFieldCardinalityType.fromString(""); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
		try { DataFieldCardinalityType.fromString("neco"); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
	}
	
	public void testGetDisplayName() {
		assertEquals("single", DataFieldCardinalityType.SINGLE.getDisplayName());
		assertEquals("list", DataFieldCardinalityType.LIST.getDisplayName());
		assertEquals("map", DataFieldCardinalityType.MAP.getDisplayName());
	}
	
	public void testGetDisplayNames() {
		String[] names = DataFieldCardinalityType.getDisplayNames();
		List<String> namesList = Arrays.asList(names);
		assertEquals(3, names.length);
		assertTrue(namesList.contains("single"));
		assertTrue(namesList.contains("list"));
		assertTrue(namesList.contains("map"));
	}
	
}
