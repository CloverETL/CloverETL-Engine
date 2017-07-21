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
package org.jetel.util.spreadsheet;

import java.awt.Point;

import org.jetel.test.CloverTestCase;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30 Aug 2011
 */
public class SpreadsheetUtilTest extends CloverTestCase {
	
	public void testGetColumnIndex() {
		assertEquals(0, SpreadsheetUtils.getColumnIndex("A"));
		assertEquals(0, SpreadsheetUtils.getColumnIndex("Aasd54"));
		assertEquals(-1, SpreadsheetUtils.getColumnIndex("?_7A"));
		assertEquals(-1, SpreadsheetUtils.getColumnIndex(""));
		assertEquals(25, SpreadsheetUtils.getColumnIndex("Z"));
		assertEquals(26, SpreadsheetUtils.getColumnIndex("AA"));
		assertEquals(2730, SpreadsheetUtils.getColumnIndex("DAA"));
		assertEquals(SpreadsheetUtils.INFINITY_COORDINATE, SpreadsheetUtils.getColumnIndex(SpreadsheetUtils.INFINITY_COORDINATE_STRING));
		assertEquals(SpreadsheetUtils.INFINITY_COORDINATE, SpreadsheetUtils.getColumnIndex(SpreadsheetUtils.INFINITY_COORDINATE_STRING + 999));
		assertEquals(new Point(SpreadsheetUtils.INFINITY_COORDINATE, 10), SpreadsheetUtils.getCellCoordinates(SpreadsheetUtils.INFINITY_COORDINATE_STRING + "11"));
		assertEquals(new Point(10, SpreadsheetUtils.INFINITY_COORDINATE), SpreadsheetUtils.getCellCoordinates("K" + SpreadsheetUtils.INFINITY_COORDINATE_STRING));
	}

}
