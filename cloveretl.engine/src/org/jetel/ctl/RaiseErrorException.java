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

import org.jetel.component.TransformUtils;
import org.jetel.ctl.ASTnode.CLVFRaiseErrorNode;

/**
 * CLO-4084:
 * <p>
 * This class represents an exception thrown by CTL raiseError() built-in function.
 * It is a subclass of {@link TransformLangExecutorRuntimeException}, so that
 * existing code treats it equally.
 * <p>
 * The only difference is that 
 * {@link TransformLangExecutor#executeFunction(CLVFFunctionDeclaration, Object[], 
 * DataRecord[], DataRecord[]) TransformLangExecutor.executeFunction()}
 * catches the exception an re-throws it unmodified.
 * <p>
 * Use {@link TransformUtils#getMessage(Throwable) TransformUtils.getMessage()}
 * to retrieve the original error message passed to raiseError().
 * 
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26. 5. 2015
 * 
 * @see <a href="https://bug.javlin.eu/browse/CLO-4084">CLO-4084</a>
 */
public class RaiseErrorException extends TransformLangExecutorRuntimeException {

	private static final long serialVersionUID = 3522077996323609196L;
	
	private final String message;
	
	/**
	 * @param node
	 * @param message
	 */
	public RaiseErrorException(CLVFRaiseErrorNode node, String message) {
		super(node, null, "Exception raised by user: " + ((message != null) ? message : "no message"));
		this.message = message;
	}

	/**
	 * @param message
	 */
	public RaiseErrorException(String message) {
		this(null, message);
	}

	/**
	 * Returns the original message passed to raiseError().
	 * 
	 * @return raiseError() message
	 */
	public String getUserMessage() {
		return message;
	}

}
