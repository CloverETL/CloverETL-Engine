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

import org.jetel.util.ExceptionUtils;

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
	 * Class name of the wrapped exception.
	 */
	private String virtualClassName;
	
	/**
	 * In case the wrapped exception is {@link CompoundException},
	 * this is list of causes of the wrapped compound exception.
	 */
	private List<SerializableException> causes;

	/**
	 * @return serializable exception derived from the given exception
	 */
	public static SerializableException wrap(Throwable e) {
		SerializableException result;
		if (e instanceof SerializableException) {
			result = (SerializableException) e; 
		} else if (e instanceof ConfigurationException) {
			result = new GraphElementSerializableException((ConfigurationException) e);
		} else if (e instanceof ComponentNotReadyException) {
			result = new GraphElementSerializableException((ComponentNotReadyException) e);
		} else {
			result = new SerializableException(e);
		}
		return result;
	}
	
	SerializableException(Throwable e) {
		if (e.getCause() != null) {
			//if the wrapped exception has a cause exception
			//the cause is wrapped as well and set as cause of this SerializableException
			initCause(wrap(e.getCause()));
		}
		
		//persist message of the wrapped exception
		message = e.getMessage();
		//persist class name of the wrapped exception
		virtualClassName = e.getClass().getName();
		//inherit stacktrace from the wrapped exception
		setStackTrace(e.getStackTrace());

		//persist suppressed exceptions
		for (Throwable suppressedException : e.getSuppressed()) {
			addSuppressed(wrap(suppressedException));
		}

		//compound exception needs special handling
		if (e instanceof CompoundException) {
			CompoundException ce = (CompoundException) e;
			causes = new ArrayList<SerializableException>(ce.getCauses().size());
			for (Throwable t : ce.getCauses()) {
				causes.add(wrap(t));
			}
		}
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
        return (message != null) ? (virtualClassName + ": " + message) : virtualClassName;
	}

	/**
	 * This is workaround for regular instanceof operator.
	 */
	public boolean instanceOf(Class<? extends Throwable> throwableClass) {
		try {
			Class<?> virtualClass = Class.forName(virtualClassName);
			return throwableClass.isAssignableFrom(virtualClass);
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	/**
	 * @return class name of wrapped exception
	 */
	public String getWrappedExceptionClassName() {
		return virtualClassName;
	}

	/**
	 * @return non empty list of causes only for wrapped {@link CompoundException} 
	 */
	public List<SerializableException> getCauses() {
		return causes;
	}
	
}
