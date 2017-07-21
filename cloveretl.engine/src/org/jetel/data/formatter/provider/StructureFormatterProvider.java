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
import org.jetel.data.formatter.StructureFormatter;

/**
 * Provides support for getting the structure data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class StructureFormatterProvider implements FormatterProvider {

	private String charEncoder;
	private String header;
	private String footer;
	private String charSet;
	private String mask;
	
	/**
	 * Contructors.
	 */
	public StructureFormatterProvider() {
	}
	public StructureFormatterProvider(String charEncoder) {
		this.charEncoder = charEncoder;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	@Override
	public Formatter getNewFormatter() {
		StructureFormatter formatter;
		if (charEncoder == null) {
			formatter =	new StructureFormatter();
		} else {
			formatter =	new StructureFormatter(charEncoder);
		}
		formatter.setHeader(header);
		formatter.setFooter(footer);
		formatter.setMask(mask);
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

	public void setMask(String mask) {
		this.mask = mask;
	}

    public void setFooter(String footer) {
    	this.footer = footer;
    }

}
