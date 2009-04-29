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
