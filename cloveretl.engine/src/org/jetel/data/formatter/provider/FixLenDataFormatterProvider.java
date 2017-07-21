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

import org.jetel.data.formatter.FixLenDataFormatter;
import org.jetel.data.formatter.Formatter;

/**
 * Provides support for getting the lixlen data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class FixLenDataFormatterProvider implements FormatterProvider {

	private String charEncoder;
	private String header;
	private Character chRecordFiller;
	private Character chFieldFiller;
	private Character recordFiller;
	private Character fieldFiller;
	private String charSet;
	private Boolean chLeftAlign;
	private Boolean leftAlign;
	
	/**
	 * Contructors.
	 */
	public FixLenDataFormatterProvider() {
	}
	public FixLenDataFormatterProvider(String charEncoder) {
		this.charEncoder = charEncoder;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	@Override
	public Formatter getNewFormatter() {
		FixLenDataFormatter formatter;
		if (charEncoder == null) {
			formatter =	new FixLenDataFormatter();
		} else {
			formatter =	new FixLenDataFormatter(charEncoder);
		}
		formatter.setHeader(header);
		formatter.setRecordFiller(chRecordFiller);
		formatter.setFieldFiller(chFieldFiller);
		if (chLeftAlign != null) {
			formatter.setLeftAlign(chLeftAlign.booleanValue());
		}
		recordFiller = formatter.getRecordFiller();
		fieldFiller = formatter.getFieldFiller();
		charSet = formatter.getCharSetName();
		leftAlign = formatter.isLeftAlign();
		return formatter;
	}

	/**
	 * Gets charset.
	 * 
	 * @return
	 */
	public String getCharSetName() {
		return charSet;
	}

	/**
	 * Sets header.
	 * 
	 * @param header
	 */
	public void setHeader(String header) {
    	this.header = header;
    }
	
	public void setRecordFiller(Character filler) {
		this.chRecordFiller = filler;
	}

    public Character getRecordFiller() {
        return recordFiller;
    }

	public void setFieldFiller(Character filler) {
        this.chFieldFiller = filler;
	}

    public Character getFieldFiller() {
        return fieldFiller;
    }

    public void setLeftAlign(Boolean leftAlign) {
		this.chLeftAlign = leftAlign;
	}
    
	public Boolean isLeftAlign() {
		return leftAlign;
	}
}
