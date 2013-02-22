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
package org.jetel.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.jetel.exception.CompoundException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.StackTraceWrapperException;
import org.jetel.util.string.StringUtils;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.2.2013
 */
public class ExceptionUtils {

	/**
	 * Converts stack trace of a given throwable to a string.
	 *
	 * @param throwable a throwable
	 *
	 * @return stack trace of the given throwable as a string
	 */
	public static String stackTraceToString(Throwable throwable) {
		if (throwable == null) {
			return null;
		} else {
			StringWriter stringWriter = new StringWriter();
			throwable.printStackTrace(new PrintWriter(stringWriter));
	
			//let's look at the last exception in the chain
			while (throwable.getCause() != null && throwable.getCause() != throwable) {
				throwable = throwable.getCause();
			}
			
			//if the last exception is CompoundException, we have to print out stack traces even of these exceptions 
			if (throwable instanceof CompoundException) {
				for (Throwable innerThrowable : ((CompoundException) throwable).getCauses()) {
					stringWriter.append("\n");
					stringWriter.append(stackTraceToString(innerThrowable));
				}
			}
			
			//StackTraceWrapperException has to be handled in special way - stacktrace of a cause is stored in local attribute
			if (throwable instanceof StackTraceWrapperException) {
				String causeStackTrace = ((StackTraceWrapperException) throwable).getCauseStackTrace();
				if (causeStackTrace != null) {
					stringWriter.append("Caused by: " + causeStackTrace);
				}
			}
			return stringWriter.toString();
		}
	}

    /**
     * Extract message from the given exception chain. All messages from all exceptions are concatenated
     * to the resulted string.
     * @param exception converted exception
     * @return resulted overall message
     */
    public static String exceptionChainToMessage(Throwable exception) {
    	return exceptionChainToMessage(null, exception);
    }

    /**
     * Extract message from the given exception chain. All messages from all exceptions are concatenated
     * to the resulted string.
     * @param message prefixed message text which will be in the start of resulted string
     * @param exception converted exception
     * @return resulted overall message
     */
    public static String exceptionChainToMessage(String message, Throwable exception) {
    	return exceptionChainToMessage(new JetelRuntimeException(message, exception), 0);
    }

    /**
     * Extract message from the given exception chain. All messages from all exceptions are concatenated
     * to the resulted string.
     * @param message prefixed message text which will be in the start of resulted string
     * @param exception converted exception
     * @return resulted overall message
     */
    private static String exceptionChainToMessage(Throwable exception, int depth) {
    	StringBuilder result = new StringBuilder();
    	Throwable exceptionIterator = exception;
    	String lastMessage = "";
    	while (true) {
    		//extract message from current exception
    		String newMessage = getMessage(exceptionIterator, lastMessage);
    		
    		if (newMessage != null) {
    			appendMessage(result, newMessage, depth);
	    		depth++;
	    		lastMessage = newMessage;
    		}

    		//CompoundException needs special handling
    		if (exceptionIterator instanceof CompoundException) {
    			for (Throwable t : ((CompoundException) exceptionIterator).getCauses()) {
    				String messageOfNestedException = exceptionChainToMessage(t, depth);
    				if (!StringUtils.isEmpty(messageOfNestedException)) {
	        			if (!StringUtils.isEmpty(result)) {
	        				result.append("\n");
	        			}
	        			result.append(messageOfNestedException);
    				}
    			}
    			break;
    		}
    		
    		if (exceptionIterator.getCause() == null || exceptionIterator.getCause() == exceptionIterator) {
    			break;
    		} else {
    			exceptionIterator = exceptionIterator.getCause();
    		}
    	}
    	return result.toString();
    }
    
    private static String getMessage(Throwable t, String lastMessage) {
    	String message = null;
    	//NPE is handled in special way
		if (t instanceof NullPointerException 
				&& (StringUtils.isEmpty(t.getMessage()) || t.getMessage().equalsIgnoreCase("null"))) {
			//in case the NPE has no reasonable message, we append more descriptive error message
			message = "Unexpected null value.";
		} else if (!StringUtils.isEmpty(t.getMessage())) {
			//only non-empty messages are considered
			message = t.getMessage();
		}
		
		//in case the previous message is identical, this message is skipped
		if (EqualsUtil.areEqual(message, lastMessage)) {
			message = null;
		}
		
		//in case the exception was created with "new Throwable(Throwable cause)" constructor
		//generic message of this exception is useless, since all necessary information are in cause
		//and this is attempt to detect it and skip it in the message stack
		Throwable cause = t.getCause();
		if (message != null && cause != null
				&&
			 (message.equals(cause.getClass().getName())
					 || message.equals(cause.getClass().getName() + ": " + cause.getMessage()))) {
				 message = null;
		}
		
		return message;
    }
    
	private static void appendMessage(StringBuilder result, String message, int depth) {
		String[] messageLines = message.split("\\n");
		for (String messageLine : messageLines) {
			if (!StringUtils.isEmpty(result)) {
				result.append("\n");
			}
			for (int i = 0; i < depth; i++) {
				result.append(" ");
			}
			result.append(messageLine);
		}
	}

	/**
	 * Print out the given exception in preferred form into the given logger. 
	 * @param logger
	 * @param message
	 * @param t
	 */
	public static void logException(Logger logger, String message, Throwable t) {
        logger.error(ExceptionUtils.exceptionChainToMessage(message, t));
        logger.error("Error details:\n" + ExceptionUtils.stackTraceToString(t));
	}
	
	/**
	 * Check whether the given exception or one of its children is instance of a given class.
	 * @param t exception to be searched
	 * @param exceptionClass searched exception type 
	 * @return true if the given exception or some of its children is instance of a given class.
	 */
	public static boolean instanceOf(Throwable t, Class<? extends Throwable> exceptionClass) {
		while (t != null) {
			if (exceptionClass.isInstance(t)) {
				return true;
			}
			if (t instanceof CompoundException) {
				for (Throwable cause : ((CompoundException) t).getCauses()) {
					if (instanceOf(cause, exceptionClass)) {
						return true;
					}
				}
				return false;
			}
			if (t != t.getCause()) {
				t = t.getCause();
			} else {
				t = null;
			}
		}
		return false;
	}
	
	/**
	 * Returns list of all exceptions of a given type in the given exception chain.
	 * @param t root of searched exception chain
	 * @param exceptionClass searched exception type
	 * @return list of exceptions with a given type in the given exception chain
	 */
	public static <T extends Throwable> List<T> getAllExceptions(Throwable t, Class<T> exceptionClass) {
		List<T> result = new ArrayList<T>();
		
		while (t != null) {
			if (exceptionClass.isInstance(t)) {
				result.add(exceptionClass.cast(t));
			}
			if (t instanceof CompoundException) {
				for (Throwable cause : ((CompoundException) t).getCauses()) {
					result.addAll(getAllExceptions(cause, exceptionClass));
				}
				return result;
			}
			if (t != t.getCause()) {
				t = t.getCause();
			} else {
				t = null;
			}
		}
		
		return result;
	}
	
}
