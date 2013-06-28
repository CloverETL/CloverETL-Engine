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
public class DataFieldContainerTypeTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	public void testFromString() {
		assertEquals(DataFieldContainerType.SINGLE, DataFieldContainerType.fromString(null));
		assertEquals(DataFieldContainerType.SINGLE, DataFieldContainerType.fromString(""));
		assertEquals(DataFieldContainerType.LIST, DataFieldContainerType.fromString("list"));
		assertEquals(DataFieldContainerType.MAP, DataFieldContainerType.fromString("MAP"));
		
		try { DataFieldContainerType.fromString("neco"); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
	}
	
	public void testGetDisplayName() {
		assertEquals("", DataFieldContainerType.SINGLE.getDisplayName());
		assertEquals("list", DataFieldContainerType.LIST.getDisplayName());
		assertEquals("map", DataFieldContainerType.MAP.getDisplayName());
	}
	
	public void testGetDisplayNames() {
		String[] names = DataFieldContainerType.getDisplayNames();
		List<String> namesList = Arrays.asList(names);
		assertEquals(3, names.length);
		assertTrue(namesList.contains(""));
		assertTrue(namesList.contains("list"));
		assertTrue(namesList.contains("map"));
	}
	
	public void testGetByteIdentifier() {
		assertEquals((byte) 0, DataFieldContainerType.SINGLE.getByteIdentifier());
		assertEquals((byte) 1, DataFieldContainerType.LIST.getByteIdentifier());
		assertEquals((byte) 2, DataFieldContainerType.MAP.getByteIdentifier());
	}
	
	public void testFromByteIdentifier() {
		assertEquals(DataFieldContainerType.SINGLE, DataFieldContainerType.fromByteIdentifier((byte) 0));
		assertEquals(DataFieldContainerType.LIST, DataFieldContainerType.fromByteIdentifier((byte) 1));
		assertEquals(DataFieldContainerType.MAP, DataFieldContainerType.fromByteIdentifier((byte) 2));
		try {
			DataFieldContainerType.fromByteIdentifier((byte) 3);
			assertTrue(false);
		} catch (Exception e) {
			//OK
		}
		try {
			DataFieldContainerType.fromByteIdentifier((byte) -1);
			assertTrue(false);
		} catch (Exception e) {
			//OK
		}
	}
	
}
