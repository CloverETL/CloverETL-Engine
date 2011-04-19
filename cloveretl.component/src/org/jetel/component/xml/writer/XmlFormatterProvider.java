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
package org.jetel.component.xml.writer;

import org.jetel.component.xml.writer.model.WritableMapping;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.provider.FormatterProvider;

/**
 * @author LKREJCI (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 18 Jan 2011
 */
public class XmlFormatterProvider implements FormatterProvider {

	private final WritableMapping mapping;
	private final boolean omitNewLines;
	private final String encoding;

	public XmlFormatterProvider(WritableMapping mapping, boolean omitNewLines, String encoding) {
		this.mapping = mapping;
		this.omitNewLines = omitNewLines;
		this.encoding = encoding;
	}

	@Override
	public Formatter getNewFormatter() {
		return new XmlFormatter(mapping, omitNewLines, encoding);
	}
}
