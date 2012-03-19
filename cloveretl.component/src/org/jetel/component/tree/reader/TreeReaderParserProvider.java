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
package org.jetel.component.tree.reader;

import org.jetel.component.TreeReader.InputAdapter;


/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19 Jan 2012
 */
public interface TreeReaderParserProvider {

	/**
	 * Indicates whether this instance provides {@link TreeStreamParser}.
	 * One of {@link #providesTreeStreamParser()} and {@link #providesXPathEvaluator()} methods has to return true.
	 * @return true iff {@link #getTreeStreamParser()} does not return null.
	 */
	boolean providesTreeStreamParser();
	
	/**
	 * Provides parser which reads a tree of some concrete structure/format and during the process 
	 * it generates evens which describe such a tree in a general way. Used for conversions between tree formats.
	 * Called only if {@link #providesTreeStreamParser()} returns true.
	 * @return tree parser.
	 */
	TreeStreamParser getTreeStreamParser();
	
	/**
	 * Provides a handler able to fill Clover data fields with some value read from a tree.
	 * @return a handler. Must not be null.
	 */
	ValueHandler getValueHandler();
	
	/**
	 * Signals if this instance provides {@link XPathEvaluator}.
	 * One of {@link #providesTreeStreamParser()} and {@link #providesXPathEvaluator()} methods has to return true.
	 * @return true iff {@link #getXPathEvaluator()} does not return null. 
	 */
	boolean providesXPathEvaluator();
	
	/**
	 * Provides {@link XPathEvaluator} which evaluates XPath expressions directly on a tree of some concrete format.
	 * @return an evaluator. Or null if {@link #providesXPathEvaluator()} returns false.
	 */
	XPathEvaluator getXPathEvaluator();
	
	/**
	 * Provides adapter which processes input provided by SourceIterator into a form understood by the {@link XPathEvaluator}
	 * provided by this instance (the "input" is is refereed to as "evaluationContext" in the {@link XPathEvaluator}).
	 * @return an input adapter. Must not be null.
	 */
	InputAdapter getInputAdapter();
	
}
