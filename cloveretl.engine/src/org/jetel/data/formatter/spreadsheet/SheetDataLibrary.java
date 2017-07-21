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
package org.jetel.data.formatter.spreadsheet;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class SheetDataLibrary {
	private Map<String, SheetData> sheetNameToSheetDataMap = new HashMap<String, SheetData>();
	
	public SheetData addSheetData(String sheetName, SheetData sheetData) {
		return sheetNameToSheetDataMap.put(sheetName, sheetData);
	}
	
	public SheetData getSheetData(String sheetName) {
		return sheetNameToSheetDataMap.get(sheetName);
	}
	
	public Set<String> getSheetNames() {
		return sheetNameToSheetDataMap.keySet();
	}
	
	public Collection<SheetData> getAllSheetData() {
		return sheetNameToSheetDataMap.values();
	}
	
	public void clear() {
		sheetNameToSheetDataMap.clear();
	}
}