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
package org.jetel.component.tree.reader.xml;

import org.jetel.component.TreeReader.InputAdapter;
import org.jetel.component.TreeReader.ReadableByteChannelToSourceAdapter;
import org.jetel.component.tree.reader.TreeReaderParserProvider;
import org.jetel.component.tree.reader.TreeStreamParser;
import org.jetel.component.tree.reader.ValueHandler;
import org.jetel.component.tree.reader.XPathEvaluator;

/**
 * @author krejcil (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.2.2012
 */
public class XmlReaderParserProvider implements TreeReaderParserProvider {

	private String charset;
	
	public XmlReaderParserProvider(String charset) {
		this.charset = charset;
	}

	@Override
	public boolean providesTreeStreamParser() {
		return false;
	}

	@Override
	public TreeStreamParser getTreeStreamParser() {
		return null;
	}

	@Override
	public ValueHandler getValueHandler() {
		return new XmlValueHandler();
	}

	@Override
	public boolean providesXPathEvaluator() {
		return true;
	}

	@Override
	public XPathEvaluator getXPathEvaluator() {
		return new XmlXPathEvaluator();
	}

	@Override
	public InputAdapter getInputAdapter() {
		return new ReadableByteChannelToSourceAdapter(charset);
	}
}
