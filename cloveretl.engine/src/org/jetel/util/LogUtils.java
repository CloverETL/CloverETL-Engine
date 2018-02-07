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
package org.jetel.util;

import org.apache.log4j.Logger;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12 Apr 2012
 */
public class LogUtils {

	public static final String MDC_RUNID_KEY = "runId";

	private static final Logger DEFAULT_LOGGER = Logger.getLogger(LogUtils.class);
	
	public static Logger getLogger() {
		Node component = ContextProvider.getNode();
		if (component != null) {
			return component.getLog();
		}

		TransformationGraph graph = ContextProvider.getGraph();
		if (graph != null) {
			return graph.getLog();
		}
		
		return DEFAULT_LOGGER;
	}

	/**
	 * Converts the given record to a string for logging purpose.
	 * The result will be one-line string form of data record.  
	 * @param record converted data record
	 * @return simple string representation of the given record
	 */
	public static String toString(DataRecord record) {
		StringBuffer str = new StringBuffer(80);
		str.append('{');
		for (DataField field : record) {
			str.append(field.getMetadata().getName());
			str.append("=");
			str.append(field.toString());
			str.append(", ");
		}
		if (str.length() > 1) {
			str.setLength(str.length() - 1);
		}
		str.append('}');
		return str.toString();
	}
	
}
