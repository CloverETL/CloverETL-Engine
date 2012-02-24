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
package org.jetel.component.tree;

import org.jetel.component.xpathparser.XPathEvaluator;
import org.jetel.component.xpathparser.xml.XmlValueHandler;
import org.jetel.data.tree.json.parser.JsonStreamParser;
import org.jetel.data.tree.parser.TreeStreamParser;
import org.jetel.data.tree.parser.ValueHandler;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19 Jan 2012
 */
public class JsonReaderParserProvider implements TreeReaderParserProvider {

	@Override
	public boolean providesTreeStreamParser() {
		return true;
	}

	@Override
	public TreeStreamParser getTreeStreamParser() {
		return new JsonStreamParser();
	}

	@Override
	public ValueHandler getValueHandler() {
		return new XmlValueHandler();
	}

	@Override
	public boolean providesXPathEvaluator() {
		return false;
	}

	@Override
	public XPathEvaluator getXPathEvaluator() {
		return null;
	}

}
