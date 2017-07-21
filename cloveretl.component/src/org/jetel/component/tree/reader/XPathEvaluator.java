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

import java.util.Iterator;
import java.util.Map;

import org.jetel.component.tree.reader.mappping.MappingElement;

/**
 * Contract to evaluate an XPath expression in some context.
 * 
 * @author jan.michalica (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 5.12.2011
 */
public interface XPathEvaluator {

	Iterator<Object> iterate(String xpath, Map<String, String> namespaceBinding, Object evaluationContext,
			MappingElement mapping);

	Object evaluatePath(String xpath, Map<String, String> namespaceBinding, Object evaluationContext,
			MappingElement mapping);

	Object evaluateNodeName(String nodeName, Map<String, String> namespaceBinding, Object evaluationContext,
			MappingElement mapping);

	/**
	 * Called once one parsing process is finished.
	 */
	void reset();
}
