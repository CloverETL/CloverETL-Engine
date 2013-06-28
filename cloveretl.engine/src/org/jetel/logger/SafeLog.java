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
package org.jetel.logger;

import org.apache.commons.logging.Log;

/** Handles logging of messages that may contain sensitive information. 
 *  It provides various methods for logging, based on the data contained in the log.
 * 
 *  Methods fall in one of the following category:
 *  <ul>
 *  <li>(debug|info|warn|error|fatal) - output's message to a standard log. No explicit obfuscation takes place, it must be handled by layout</li>
 *  <li>(debug|info|warn|error|fatal)Obfuscated - if sensitive log is disabled, the output message is obfuscated and logged, otherwise full message is logged</li>
 *  <li>(debug|info|warn|error|fatal) with 2 log messages in argument - allows to specify a log message in case, that the sensitive log is disabled/enabled</li>
 *  </ul>
 *  
 * 
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.2.2012
 */
public class SafeLog implements Log {
	/** Log, where all standard log goes (the messages should not contain sensitive information) */
	private Log standardLog;

	/** Log, where the access information can be logged. */
	private Log sensitiveLog;

	/** Constructor for safe log. It takes two logs as the argument:
	 * <ul>
	 *  <li>standard - standard log messages, without sensitive information</li>
	 *  <li>sensitive - all log messages, even with sensitive information</li>
	 * </ul>  
	 * 
	 * @param standardLog
	 * @param sensitiveLog
	 */
	SafeLog(Log standardLog, Log sensitiveLog) {
		this.standardLog = standardLog;
		this.sensitiveLog = sensitiveLog;
	}

	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 */
	public void debugObfuscated(String message) {
		debug(SafeLogUtils.obfuscateSensitiveInformation(message), message);
	}

	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 * @param throwable
	 */
	public void debugObfuscated(String message, Throwable throwable) {
		debug(SafeLogUtils.obfuscateSensitiveInformation(message), message, throwable);
	}

	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 */
	public void errorObfuscated(String message) {
		error(SafeLogUtils.obfuscateSensitiveInformation(message), message);
	}

	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 * @param throwable
	 */
	public void errorObfuscated(String message, Throwable throwable) {
		error(SafeLogUtils.obfuscateSensitiveInformation(message), message);
	}

	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 */
	public void fatalObfuscated(String message) {
		fatal(SafeLogUtils.obfuscateSensitiveInformation(message), message);
	}

	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 * @param throwable
	 */
	public void fatalObfuscated(String message, Throwable throwable) {
		fatal(SafeLogUtils.obfuscateSensitiveInformation(message), message);
	}

	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 */
	public void infoObfuscated(String message) {
		info(SafeLogUtils.obfuscateSensitiveInformation(message), message);
	}

	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 * @param throwable
	 */
	public void infoObfuscated(String message, Throwable throwable) {
		info(SafeLogUtils.obfuscateSensitiveInformation(message), message);
	}	
	
	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 */
	public void warnObfuscated(String message) {
		warn(SafeLogUtils.obfuscateSensitiveInformation(message), message);
	}

	/** Logs an obfuscated message if the sensitive log is not enabled. Otherwise 
	 *  full message is logged.
	 * 
	 * @param message
	 * @param throwable
	 */
	public void warnObfuscated(String message, Throwable throwable) {
		warn(SafeLogUtils.obfuscateSensitiveInformation(message), message);
	}	
	
	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 */
	public void debug(Object standard, Object sensitive) {
		if (sensitiveLog.isDebugEnabled()) {
			sensitiveLog.debug(sensitive);
		} else {
			standardLog.debug(standard);
		}
	}

	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 * @param throwable
	 */
	public void debug(Object standard, Object sensitive, Throwable throwable) {
		if (sensitiveLog.isDebugEnabled()) {
			sensitiveLog.debug(sensitive, throwable);
		} else {
			standardLog.debug(standard, throwable);
		}
	}

	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 */	
	public void error(Object standard, Object sensitive) {
		if (sensitiveLog.isErrorEnabled()) {
			sensitiveLog.error(sensitive);
		} else {
			standardLog.error(standard);
		}
	}

	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 * @param throwable
	 */
	public void error(Object standard, Object sensitive, Throwable throwable) {
		if (sensitiveLog.isErrorEnabled()) {
			sensitiveLog.error(sensitive, throwable);
		} else {
			standardLog.error(standard, throwable);
		}	
	}

	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 */
	public void fatal(Object standard, Object sensitive) {
		if (sensitiveLog.isFatalEnabled()) {
			sensitiveLog.fatal(sensitive);
		} else {
			standardLog.fatal(standard);
		}	
	}

	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 * @param throwable
	 */
	public void fatal(Object standard, Object sensitive, Throwable throwable) {
		if (sensitiveLog.isFatalEnabled()) {
			sensitiveLog.fatal(sensitive, throwable);
		} else {
			standardLog.fatal(standard, throwable);
		}	
	}

	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 */
	public void info(Object standard, Object sensitive) {
		if (sensitiveLog.isInfoEnabled()) {
			sensitiveLog.info(sensitive);
		} else {
			standardLog.info(standard);
		}	
	}

	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 * @param throwable
	 */
	public void info(Object standard, Object sensitive, Throwable throwable) {
		if (sensitiveLog.isInfoEnabled()) {
			sensitiveLog.info(sensitive, throwable);
		} else {
			standardLog.info(standard, throwable);
		}	
	}
	
	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 */
	public void warn(Object standard, Object sensitive) {
		if (sensitiveLog.isInfoEnabled()) {
			sensitiveLog.warn(sensitive);
		} else {
			standardLog.warn(standard);
		}	
	}

	/** Logs a standard message if the sensitive log is not enabled. Otherwise 
	 *  sensitive message is logged.
	 * 
	 * @param standard - message to be logged if the sensitive log is disabled
	 * @param sensitive - message to be logged if the sensitive log is enabled
	 * @param throwable
	 */
	public void warn(Object standard, Object sensitive, Throwable throwable) {
		if (sensitiveLog.isInfoEnabled()) {
			sensitiveLog.warn(sensitive, throwable);
		} else {
			standardLog.warn(standard, throwable);
		}	
	}	

	@Override
	public void trace(Object standard) {
		standardLog.trace(standard);
	}

	@Override
	public void trace(Object standard, Throwable throwable) {
		standardLog.trace(standard, throwable);
	}

	@Override
	public void warn(Object standard) {
		standardLog.warn(standard);
	}

	@Override
	public void warn(Object standard, Throwable throwable) {
		standardLog.warn(standard, throwable);
	}

	@Override
	public void debug(Object standard) {
		standardLog.debug(standard);
	}

	@Override
	public void debug(Object standard, Throwable throwable) {
		standardLog.debug(standard, throwable);	
	}

	@Override
	public void error(Object standard) {
		standardLog.error(standard);
	}

	@Override
	public void error(Object standard, Throwable throwable) {
		standardLog.error(standard, throwable);
	}

	@Override
	public void fatal(Object standard) {
		standardLog.fatal(standard);		
	}

	@Override
	public void fatal(Object standard, Throwable throwable) {
		standardLog.fatal(standard, throwable);
	}

	@Override
	public void info(Object standard) {
		standardLog.info(standard);
	}

	@Override
	public void info(Object standard, Throwable throwable) {
		standardLog.info(standard, throwable);
	}

	@Override
	public boolean isDebugEnabled() {
		return standardLog.isDebugEnabled();
	}

	@Override
	public boolean isErrorEnabled() {
		return standardLog.isErrorEnabled();
	}

	@Override
	public boolean isFatalEnabled() {
		return standardLog.isFatalEnabled();
	}

	@Override
	public boolean isInfoEnabled() {
		return standardLog.isInfoEnabled();
	}

	@Override
	public boolean isTraceEnabled() {
		return standardLog.isTraceEnabled();
	}

	@Override
	public boolean isWarnEnabled() {
		return standardLog.isWarnEnabled();
	}	
}
