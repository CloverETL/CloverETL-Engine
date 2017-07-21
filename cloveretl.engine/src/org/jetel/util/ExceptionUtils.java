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
    public static String getMessage(Throwable exception) {
    	return getMessage(null, exception);
    }

    /**
     * Extract message from the given exception chain. All messages from all exceptions are concatenated
     * to the resulted string.
     * @param message prefixed message text which will be in the start of resulted string
     * @param exception converted exception
     * @return resulted overall message
     */
    public static String getMessage(String message, Throwable exception) {
    	List<ErrorMessage> errMessages = getMessages(new JetelRuntimeException(message, exception), 0);
    	StringBuilder result = new StringBuilder();
    	for (ErrorMessage errMessage : errMessages) {
    		appendMessage(result, errMessage.message, errMessage.depth);
    	}
    	return result.toString();
    }

    private static class ErrorMessage {
    	int depth;
    	
    	String message;

    	public ErrorMessage(int depth, String message) {
    		this.depth = depth;
    		this.message = message;
		}
    }
    
    /**
     * Extract message from the given exception chain. All messages from all exceptions are concatenated
     * to the resulted string.
     * @param message prefixed message text which will be in the start of resulted string
     * @param exception converted exception
     * @return resulted overall message
     */
    private static List<ErrorMessage> getMessages(Throwable exception, int depth) {
    	List<ErrorMessage> result = new ArrayList<ErrorMessage>();
    	Throwable exceptionIterator = exception;
    	String lastMessage = "";
    	while (true) {
    		//extract message from current exception
    		String newMessage = getMessageNonRecurisve(exceptionIterator, lastMessage);
    		
    		if (newMessage != null) {
    			result.add(new ErrorMessage(depth, newMessage));
	    		depth++;
	    		lastMessage = newMessage;
    		}

    		//CompoundException needs special handling
    		if (exceptionIterator instanceof CompoundException) {
    			for (Throwable t : ((CompoundException) exceptionIterator).getCauses()) {
    				result.addAll(getMessages(t, depth));
    			}
    			break;
    		}
    		
    		if (exceptionIterator.getCause() == null || exceptionIterator.getCause() == exceptionIterator) {
    			break;
    		} else {
    			exceptionIterator = exceptionIterator.getCause();
    		}
    	}
    	return result;
    }
    
    private static String getMessageNonRecurisve(Throwable t, String lastMessage) {
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
		
		//do not report exception message that is mentioned already in parent exception message
		if (message != null && lastMessage != null && lastMessage.contains(message)) {
			message = null;
		}
		
		//in case the exception was created with "new Throwable(Throwable cause)" constructor
		//generic message of this exception is useless, since all necessary information are in cause
		//and this is attempt to detect it and skip it in the message stack
		// FIXME: UnknownHostException("bla.bla") will be reduced to "bla.bla" with the "unknown host" info missing
		// sometimes we cannot affect this at all, as in the case of http-client library, see HttpConnector
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
		String completeMessage = ExceptionUtils.getMessage(message, t);
		if (!StringUtils.isEmpty(completeMessage)) {
			logger.error(completeMessage);
		}
		if (t != null) {
			logger.error("Error details:\n" + ExceptionUtils.stackTraceToString(t));
		}
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

	/**
	 * Print given message to logger. The message is surrounded in an ascii-art frame.  
	 * @param message printed message
	 */
	public static void logHighlightedMessage(Logger logger, String message) {
		final String LEFT_BORDER = "  ";
		final String RIGHT_BORDER = "";
		final char TOP_BORDER = '-';
		final String TOP_BORDER_LABEL = " Error details ";
		final char BOTTOM_BORDER = '-';
		int TOP_BORDER_LABEL_LOCATION = 3;
		
		if (!StringUtils.isEmpty(message)) {
			StringBuilder sb = new StringBuilder("\n");
			//split message to separate lines
			String[] messageLines = message.split("\n");
			//find the longest message line
			int maxLength = 80;
			for (String messageLine : messageLines) {
				if (messageLine.length() > maxLength) {
					maxLength = messageLine.length();
				}
			}
			TOP_BORDER_LABEL_LOCATION = (maxLength / 2) - (TOP_BORDER_LABEL.length() / 2);
			
			//create header line of error message
			for (int i = 0; i < maxLength + LEFT_BORDER.length() + RIGHT_BORDER.length(); i++) {
				if (i >= TOP_BORDER_LABEL_LOCATION && i < TOP_BORDER_LABEL.length() + TOP_BORDER_LABEL_LOCATION) {
					sb.append(TOP_BORDER_LABEL.charAt(i - TOP_BORDER_LABEL_LOCATION));
				} else {
					sb.append(TOP_BORDER);
				}
			}
			sb.append('\n');
			//append error message lines
			for (String messageLine : messageLines) {
				sb.append(LEFT_BORDER);
				sb.append(messageLine);
				if (RIGHT_BORDER.length() > 0) {
					for (int i = messageLine.length(); i < maxLength; i++) {
						sb.append(' ');
					}
					sb.append(RIGHT_BORDER);
				}
				sb.append('\n');
			}
			//append footer line of error message
			for (int i = 0; i < maxLength + LEFT_BORDER.length() + RIGHT_BORDER.length(); i++) {
				sb.append(BOTTOM_BORDER);
			}
			logger.error(sb.toString());
		}
	}

	/**
	 * Print out error message of the given exception. The message is surrounded in an ascii-art frame.
	 * @param logger
	 * @param message
	 * @param exception
	 */
	public static void logHighlightedException(Logger logger, String message, Throwable exception) {
		logHighlightedMessage(logger, getMessage(message, exception));
	}
	
}
