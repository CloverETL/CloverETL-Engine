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
package org.jetel.ctl;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * An error message within CTL code.
 * 
 * @author Michal Tomcanyi, Javlin a.s. &lt;michal.tomcanyi@javlin.cz&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 * 
 * @version 27th May 2010
 * @created 6th October 2008
 * 
 * @see ProblemReporter
 */
public class ErrorMessage implements Serializable {

	private static final long serialVersionUID = 907686576819435587L;

	/** The URL of imported file this error message applies to, <code>null</code> for the main file. */
	private String importFileUrl;

	/** The error level. */
	private ErrorLevel level;
	/** The location of the error within the main file, <code>null<code> for the main file. */
	private ErrorLocation globalLocation;
	/** The location of the error within the current file. */
	private ErrorLocation localLocation;

	/** The error message itself. */
	private String errorMessage;
	/** The hint that suggest how to fix the error. */
	private String hint;
	
	/**
	 * Detailed information about the error. May be <code>null</code>
	 */
	private Detail detail;

	/**
	 * Constructs an <code>ErrorMessage</code> instance.
	 *
	 * @param importFileUrl the URL of imported file this error message applies to, <code>null</code> for the main file
	 * @param level the error level
	 * @param globalLocation the location of the error within the main file, <code>null</code> for the main file
	 * @param localLocation the location of the error within the current file
	 * @param errorMessage the error message itself
	 * @param hint the hint that suggest how to fix the error
	 */
	public ErrorMessage(String importFileUrl, ErrorLevel level, ErrorLocation globalLocation,
			ErrorLocation localLocation, String errorMessage, String hint) {
		this.importFileUrl = importFileUrl;
		this.level = level;
		this.globalLocation = globalLocation;
		this.localLocation = localLocation;
		this.errorMessage = errorMessage;
		this.hint = hint;
	}

	/**
	 * @return the URL of imported file this error message applies to, <code>null</code> for the main file.
	 */
	public ErrorLevel getErrorLevel() {
		return level;
	}

	/**
	 * @return the error level.
	 */
	public String getImportFileUrl() {
		return importFileUrl;
	}

	/**
	 * @return the location of the error within the main file, <code>null<code> for the main file.
	 */
	public ErrorLocation getLocation() {
		return (globalLocation != null) ? globalLocation : localLocation;
	}

	/**
	 * @return the location of the error within the current file.
	 */
	public ErrorLocation getLocalLocation() {
		return localLocation;
	}

	/**
	 * @return the error message itself.
	 */
	public String getErrorMessage() {
		return errorMessage;
	}

	/**
	 * @return the hint that suggest how to fix the error.
	 */
	public String getHint() {
		return hint;
	}
	
	/**
	 * @return error details
	 */
	public Detail getDetail() {
		return detail;
	}

	/**
	 * Sets error details.
	 * @param detail details of the error
	 */
	public void setDetail(Detail detail) {
		this.detail = detail;
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(level.getName());

		if (importFileUrl != null) {
			stringBuilder.append(" in '");
			stringBuilder.append(importFileUrl);
			stringBuilder.append("'");
		}

		if (localLocation != null) {
			stringBuilder.append(": ").append(localLocation);
		}

		stringBuilder.append(": ").append(errorMessage).append("!");

		if (hint != null) {
			stringBuilder.append(" [").append(hint).append(".]");
		}

		return stringBuilder.toString();
	}

	public enum ErrorLevel {

		WARN("Warning"),
		ERROR("Error");

		private final String name;

		private ErrorLevel(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

	}

	
	/**
	 * Converts given list of error messages to a string report and log all errors via commons logging.
	 * @param errors
	 * @param logger
	 * @return
	 */
	public static String listToString(List<ErrorMessage> errors, Logger logger) {
		StringBuilder sb = new StringBuilder();
		for (ErrorMessage msg : errors) {
			if (logger != null) {
				logger.error(msg.toString());
			}
			if (msg.getErrorLevel() == ErrorLevel.ERROR) {
				sb.append("\n").append(msg.toString());
			}
		}
		return sb.toString();
	}

	/**
	 * An interface providing additional information about the error.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Jul 4, 2012
	 */
	public interface Detail {
		
	}
}
