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
package org.jetel.exception;

import org.jetel.util.ExceptionUtils;


/**
 * This exception allows to wrap an existing error message and stacktrace in string form.
 * This exception is usefull whereever you need to throw an exception but the cause exception
 * is available only in string form. For example RunRecord of a failing graph provides
 * only two strings - errMessage and errException.
 * The <code>causeStackTrace</code> passed into constructor is now taken into consideration only in
 * {@link ExceptionUtils#stackTraceToString(Throwable)}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15.2.2013
 */
public class StackTraceWrapperException extends JetelRuntimeException {

	private static final long serialVersionUID = 2709547291467338174L;

	private String causeStackTrace;
	
	public StackTraceWrapperException(String message, String causeStackTrace) {
		super(message);
		this.causeStackTrace = causeStackTrace;
	}
	
	public String getCauseStackTrace() {
		return causeStackTrace;
	}
	
}
