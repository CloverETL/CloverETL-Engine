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
import java.util.Collections;
import java.util.List;

/**
 * This exception bears one or list of {@link ErrorMessage} object.
 * Currently is throed from {@link ITLCompiler#convertToJava(String, Class)}.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14.7.2010
 */
public class ErrorMessageException extends Exception {

	private static final long serialVersionUID = 1L;
	
	private List<ErrorMessage> errorMessages;
	
	public ErrorMessageException(ErrorMessage errorMessage) {
		super();
		errorMessages = new ArrayList<ErrorMessage>(1);
		errorMessages.add(errorMessage);
	}

	public ErrorMessageException(List<ErrorMessage> errorMessages) {
		super();
		this.errorMessages = errorMessages;
	}
	
	public ErrorMessage getErrorMessage() {
		return errorMessages.get(0);
	}
	
	public List<ErrorMessage> getErrorMessages() {
		return Collections.unmodifiableList(errorMessages);
	}
	
}
