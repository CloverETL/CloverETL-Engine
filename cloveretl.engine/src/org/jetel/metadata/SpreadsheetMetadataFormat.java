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

/**
 * Enumeration of formats that may be used when reading spreadsheet data.
 * 
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 2. 2016
 */
public enum SpreadsheetMetadataFormat {
	
	RAW("raw", "unformatted cell value"); // for reading raw spreadsheet data, should be ignored when writing
	
	public final String value;
	public final String description;
	
	public static final String EXCEL_FORMAT_PREFIX = DataFieldFormatType.EXCEL.getFormatPrefixWithDelimiter();
	
	private SpreadsheetMetadataFormat(String value, String description) {
		this.value = value;
		this.description = description;
	}
	
	public String getFormatString() {
		return EXCEL_FORMAT_PREFIX + this.value;
	}

	public static SpreadsheetMetadataFormat fromFormatStr(String formatStr) {
		for (SpreadsheetMetadataFormat format : SpreadsheetMetadataFormat.values()) {
			if (format.value.equalsIgnoreCase(formatStr)) {
				return format;
			}
		}
		return null;
	}
	
	
}
