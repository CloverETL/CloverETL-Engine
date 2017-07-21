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

import java.util.ArrayList;
import java.util.List;

/**
 * This exception should be used whenever you want to be sure an existing exception
 * is really serializable. This exception implementation wraps another existing exception
 * and tries to inherit all important characteristic to behave as similar as possible
 * like the former exception. For example, stacktrace elements of this exception are
 * identical with the former exception. Function {@link #getMessage()} should return
 * same message. Function {@link #toString()} should return same string representation
 * as for the former exception. Moreover, complete exception chain is wrapped in same manner.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.6.2013
 */
public class SerializableException extends JetelRuntimeException {

	private static final long serialVersionUID = -2018632846402687238L;

	/**
	 * Message of the wrapped exception.
	 */
	private String message;

	/**
	 * Class of the wrapped exception.
	 */
	private Class<? extends Throwable> virtualClass;
	
	/**
	 * In case the wrapped exception is {@link CompoundException},
	 * this is list of causes of the wrapped compound exception.
	 */
	private List<SerializableException> causes;
	
	public SerializableException(Throwable e) {
		if (e.getCause() != null) {
			//if the wrapped exception has a cause exception
			//the cause is wrapped as well and set as cause of this SerializableException
			initCause(wrapException(e.getCause()));
		}
		
		//persist message of the wrapped exception
		message = extractMessage(e);
		//persist class of the wrapped exception
		virtualClass = e.getClass();
		//inherit stackstrace from the wrapped exception
		setStackTrace(e.getStackTrace());
		
		//compound exception needs special handling
		if (e instanceof CompoundException) {
			CompoundException ce = (CompoundException) e;
			causes = new ArrayList<SerializableException>(ce.getCauses().size());
			for (Throwable t : ce.getCauses()) {
				causes.add(new SerializableException(t));
			}
		}
	}
	
	protected SerializableException wrapException(Throwable e) {
		return new SerializableException(e);
	}
	
	protected String extractMessage(Throwable e) {
		return e.getMessage();
	}
	
	@Override
	public String getMessage() {
		return message;
	}
	
	@Override
	public SerializableException getCause() {
		return (SerializableException) super.getCause();
	}
	
	@Override
	public String toString() {
        String message = getLocalizedMessage();
        return (message != null) ? (virtualClass.getName() + ": " + message) : virtualClass.getName();
	}

	/**
	 * This is workaround for regular instanceof operator.
	 */
	public boolean instanceOf(Class<? extends Throwable> throwableClass) {
		return throwableClass.isAssignableFrom(virtualClass);
	}

	/**
	 * @return class of wrapped exception
	 */
	public Class<? extends Throwable> getWrappedExceptionClass() {
		return virtualClass;
	}

	/**
	 * @return non empty list of causes only for wrapped {@link CompoundException} 
	 */
	public List<SerializableException> getCauses() {
		return causes;
	}
	
}
