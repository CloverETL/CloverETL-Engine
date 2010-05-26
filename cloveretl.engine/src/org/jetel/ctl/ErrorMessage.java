/*
 * jETeL/Clover - Java based ETL application framework.
 * Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
package org.jetel.ctl;

import java.io.Serializable;

/**
 * An error message within CTL code.
 * 
 * @author Michal Tomcanyi, Javlin a.s. &lt;michal.tomcanyi@javlin.cz&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 * 
 * @version 26th May 2010
 * @created 6th October 2008
 * 
 * @see ProblemReporter
 */
public class ErrorMessage implements Serializable {

	private static final long serialVersionUID = 3648320916057784857L;

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
	 * Constructs an <code>ErrorMessage</code> instance.
	 *
	 * @param importFileUrl the URL of imported file this error message applies to, <code>null</code> for the main file
	 * @param level the error level
	 * @param globalLocation the location of the error within the main file, <code>null<code> for the main file
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

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(level.getName());

		if (importFileUrl != null) {
			stringBuilder.append(" in '");
			stringBuilder.append(importFileUrl);
			stringBuilder.append("'");
		}

		stringBuilder.append(": ").append(localLocation);
		stringBuilder.append(": ").append(errorMessage).append(".");

		if (hint != null) {
			stringBuilder.append("[").append(hint).append(".]");
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

}
