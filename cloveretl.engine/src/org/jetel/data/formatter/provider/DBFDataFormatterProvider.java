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
package org.jetel.data.formatter.provider;

import org.jetel.data.formatter.Formatter;
import org.jetel.database.dbf.DBFDataFormatter;

/**
 * Provides support for getting DBF formatter.
 * 
 * @author Martin Slama (martin.slama@javlin.eu) (c) Javlin, a.s. (www.cloveretl.com)
 * @created May 3th 2012
 */
public class DBFDataFormatterProvider implements FormatterProvider {

	private final String charSet;
	
	private String[] excludedFieldNames;
	
	private DBFDataFormatter formatter;
	
	/**
	 * Constructor.
	 * 
	 * @param charSet Charset set of all formatters.
	 */
	public DBFDataFormatterProvider(String charSet) {
		this.charSet = charSet;
	}

	/**
	 * Creates new data formatter.
	 * @return data formatter
	 */
	@Override
	public Formatter getNewFormatter() {
		formatter = new DBFDataFormatter(charSet);
		formatter.setExcludedFieldNames(excludedFieldNames);
		return formatter;
	}

	/**
	 * @return Current instance of the formatter.
	 */
	public DBFDataFormatter getCurrentFormatter() {
		return formatter;
	}

	public String[] getExcludedFieldNames() {
		return excludedFieldNames;
	}
	
	public void setExcludedFieldNames(String[] excludedFieldNames) {
		this.excludedFieldNames = excludedFieldNames;
	}
	
	public String getCharSet() {
		return charSet;
	}
}
