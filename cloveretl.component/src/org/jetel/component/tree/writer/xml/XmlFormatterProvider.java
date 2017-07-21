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
package org.jetel.component.tree.writer.xml;

import org.jetel.component.tree.writer.BaseTreeFormatterProvider;
import org.jetel.component.tree.writer.TreeFormatter;
import org.jetel.component.tree.writer.model.runtime.WritableMapping;

/**
 * Formater provider for MultiFileWriter.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 18 Jan 2011
 */
public class XmlFormatterProvider extends BaseTreeFormatterProvider {

	private final boolean omitNewLines;
	private final String encoding;
	private final String version;
	private final boolean writeXmlDeclaration;

	public XmlFormatterProvider(WritableMapping mapping, int maxPortIndex, boolean omitNewLines, String encoding, String version, boolean writeXmlDeclaration) {
		super(mapping, maxPortIndex);
		this.omitNewLines = omitNewLines;
		this.encoding = encoding;
		this.version = version;
		this.writeXmlDeclaration = writeXmlDeclaration;
	}

	@Override
	public TreeFormatter getNewFormatter() {
		return new XmlFormatter(mapping, maxPortIndex, omitNewLines, encoding, version, writeXmlDeclaration);
	}
}
