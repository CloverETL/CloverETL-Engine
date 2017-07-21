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
package org.jetel.component;

import org.jetel.component.fileoperation.URIUtils;
import org.jetel.ctl.RaiseErrorException;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.file.SandboxUrlUtils;

/**
 * CLO-4084:
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 5. 2015
 * 
 * @see <a href="https://bug.javlin.eu/browse/CLO-4084">CLO-4084</a>
 */
public class TransformUtils {
	
	public static final String COMPONENT_ID_PARAM = "componentId"; //$NON-NLS-1$
	public static final String PROPERTY_NAME_PARAM = "propertyName"; //$NON-NLS-1$
	public static final String GRAPH_PARAMETER_NAME_PARAM = "graphParameterName"; //$NON-NLS-1$
	public static final String PLUGIN_PARAM = "plugin"; //$NON-NLS-1$
	
	/**
	 * Returns the error message passed to "onError" functions.
	 * Handles {@link RaiseErrorException} in a specific way.
	 * 
	 * @param t
	 * @return
	 */
	public static String getMessage(Throwable t) {
		if (t instanceof RaiseErrorException) {
			RaiseErrorException exception = (RaiseErrorException) t;
			return exception.getUserMessage();
		} else {
			return ExceptionUtils.getMessage(t);
		}
	}


	/**
	 * Builds source id for identification of CTL transformations and expressions. Used in debugging.
	 * @param jobUrl
	 * @param params
	 * @return
	 */
	public static String createCTLSourceId(String jobUrl, String... params) {
		StringBuilder sourceIdSB = new StringBuilder(SandboxUrlUtils.escapeUrlPath(jobUrl));
		for (int i = 0; i < params.length - 1; i+=2) {
			if (i == 0) {
				sourceIdSB.append("?");
			} else {
				sourceIdSB.append("&");
			}
			sourceIdSB.append(URIUtils.urlEncode(params[i]));
			sourceIdSB.append("=");
			sourceIdSB.append(URIUtils.urlEncode(params[i+1]));
		}
		return sourceIdSB.toString();
	}

}
