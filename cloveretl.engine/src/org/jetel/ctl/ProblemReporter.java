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

import java.util.ArrayList;
import java.util.List;

import org.jetel.ctl.ErrorMessage.ErrorLevel;

/**
 * Collector of diagnostic messages. Serves also for passing the information about error occurrence between CTL phases.
 *
 * @author Michal Tomcanyi, Javlin a.s. &lt;michal.tomcanyi@javlin.cz&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 27th May 2010
 * @created 20th November 2008
 */
public class ProblemReporter {

	/** URL of import file (being parsed) for which the error messages apply */
	private String importFileUrl = null;
	/** The error location used as global error location for imports. */
	private ErrorLocation errorLocation = null;

	/** List of diagnostic messages collected by different phases of the compiler */
	private List<ErrorMessage> diagnosticMessages = new ArrayList<ErrorMessage>();
	/** The total number of errors. */
	private int errorCount = 0;
	/** The total number of warnings. */
	private int warningCount = 0;

	/**
	 * Sets the URL of import file being currently processed. All errors reported will be related to this file. Set to
	 * <code>null</code> for 'current' file
	 * 
	 * @param importFileUrl
	 *            URL of import file being processed or <code>null</code> for current file
	 */
	public void setImportFileUrl(String importFileUrl) {
		this.importFileUrl = importFileUrl;
	}

	public String getImportFileUrl() {
		return importFileUrl;
	}

	public void setErrorLocation(ErrorLocation errorLocation) {
		this.errorLocation = errorLocation;
	}

	public ErrorLocation getErrorLocation() {
		return errorLocation;
	}

	public void warn(int beginLine, int beginColumn, int endLine, int endColumn, String error, String hint) {
		createMessage(ErrorLevel.WARN, new ErrorLocation(beginLine, beginColumn, endLine, endColumn), error, hint);
	}

	public void warn(SyntacticPosition begin, SyntacticPosition end, String error, String hint) {
		createMessage(ErrorLevel.WARN, new ErrorLocation(begin, end), error, hint);
	}

	public void error(String error, String hint) {
		createMessage(ErrorLevel.ERROR, null, error, hint);
	}
	
	public void error(int beginLine, int beginColumn, int endLine, int endColumn, String error, String hint) {
		createMessage(ErrorLevel.ERROR, new ErrorLocation(beginLine, beginColumn, endLine, endColumn), error, hint);
	}

	public void error(SyntacticPosition begin, SyntacticPosition end, String error, String hint) {
		createMessage(ErrorLevel.ERROR, new ErrorLocation(begin, end), error, hint);
	}

	private void createMessage(ErrorLevel level, ErrorLocation localErrorLocation, String error, String hint) {
		if (level == ErrorLevel.ERROR) {
			errorCount++;
		} else {
			warningCount++;
		}
		diagnosticMessages.add(new ErrorMessage(importFileUrl, level, errorLocation, localErrorLocation, error, hint));
	}

	public List<ErrorMessage> getDiagnosticMessages() {
		return new ArrayList<ErrorMessage>(diagnosticMessages);
	}

	public int errorCount() {
		return errorCount;
	}

	public int warningCount() {
		return warningCount;
	}

	public void reset() {
		importFileUrl = null;
		errorLocation = null;

		diagnosticMessages.clear();
		errorCount = 0;
		warningCount = 0;
	}

}
