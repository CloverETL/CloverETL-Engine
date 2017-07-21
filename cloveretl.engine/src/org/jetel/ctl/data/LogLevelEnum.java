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
package org.jetel.ctl.data;

import org.jetel.ctl.TransformLangParserConstants;


public enum LogLevelEnum {

	DEBUG, ERROR, FATAL, INFO, WARN, TRACE;

	public static final LogLevelEnum fromToken(int token) {
		switch (token) {
		case TransformLangParserConstants.LOGLEVEL_DEBUG:
			return DEBUG;
		case TransformLangParserConstants.LOGLEVEL_ERROR:
			return ERROR;
		case TransformLangParserConstants.LOGLEVEL_FATAL:
			return FATAL;
		case TransformLangParserConstants.LOGLEVEL_INFO:
			return INFO;
		case TransformLangParserConstants.LOGLEVEL_WARN:
			return WARN;
		case TransformLangParserConstants.LOGLEVEL_TRACE:
			return TRACE;
		default:
			throw new IllegalArgumentException("Not a log level token: " + token);
		}
	}
}
