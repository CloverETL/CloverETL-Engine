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

import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.formatter.Formatter;

/**
 * Provides support for getting the delimited data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class DelimitedDataFormatterProvider implements FormatterProvider {

	private String charEncoder;
	private String header;
	private String charSet;
	
	/**
	 * Contructors.
	 */
	public DelimitedDataFormatterProvider() {
	}
	public DelimitedDataFormatterProvider(String charEncoder) {
		this.charEncoder = charEncoder;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	@Override
	public Formatter getNewFormatter() {
		DelimitedDataFormatter formatter;
		if (charEncoder == null) {
			formatter =	new DelimitedDataFormatter();
		} else {
			formatter =	new DelimitedDataFormatter(charEncoder);
		}
		formatter.setHeader(header);
		charSet = formatter.getCharsetName();
		return formatter;
	}

	/**
	 * Sets header.
	 * 
	 * @param header
	 */
	public void setHeader(String header) {
    	this.header = header;
    }
	
	public String getCharsetName() {
		return charSet ;
	}

}
