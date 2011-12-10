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
import org.jetel.data.formatter.TextTableFormatter;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * Provides support for getting the structure data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class TextTableFormatterProvider implements FormatterProvider {

	private String charEncoder;
	private String charSet;
	private String[] mask;
	private boolean setOutputFieldNames;
	
	/**
	 * Contructors.
	 */
	public TextTableFormatterProvider() {
	}
	public TextTableFormatterProvider(String charEncoder) {
		this.charEncoder = charEncoder;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	@Override
	public Formatter getNewFormatter() {
		TextTableFormatter formatter;
		if (charEncoder == null) {
			formatter =	new TextTableFormatter();
		} else {
			formatter =	new TextTableFormatter(charEncoder);
		}
		formatter.setMask(mask);
		charSet = formatter.getCharsetName();
		formatter.setOutputFieldNames(setOutputFieldNames);
		return formatter;
	}

	public String getCharsetName() {
		return charSet ;
	}

	@SuppressWarnings(value="EI2")
	public void setMask(String[] mask) {
		this.mask = mask;
	}

	public void setOutputFieldNames(boolean setOutputFieldNames) {
		this.setOutputFieldNames = setOutputFieldNames;
	}

}
