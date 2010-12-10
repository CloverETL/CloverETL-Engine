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
package org.jetel.data.parser;

/**
 * Abstract class for all TextParsers. It contains some useful common methods.
 * 
 * @author csochor (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Dec 10, 2010
 */
public abstract class AbstractTextParser implements TextParser {
	
	final protected TextParserConfiguration cfg;

	public AbstractTextParser(TextParserConfiguration cfg) {
		super();
		this.cfg = cfg;
	}
	
	@Override
	public TextParserConfiguration getConfiguration() {
		return cfg;
	}
	
	protected boolean isSkipFieldTrailingBlanks(int fieldIndex) {
		return cfg.getSkipTrailingBlanks() != null ? cfg.getSkipTrailingBlanks() : cfg.getTrim() != null ? cfg.getTrim() : cfg.getMetadata().getField(fieldIndex).isTrim();
	}

	protected boolean isSkipFieldLeadingBlanks(int fieldIndex) {
		return cfg.getSkipLeadingBlanks() != null ? cfg.getSkipLeadingBlanks() : cfg.getTrim() != null ? cfg.getTrim() : cfg.getMetadata().getField(fieldIndex).isTrim();
	}

	
}
