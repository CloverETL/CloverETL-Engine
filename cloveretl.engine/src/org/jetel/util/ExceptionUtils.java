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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jetel.exception.CompoundException;
import org.jetel.exception.SerializableException;
import org.jetel.exception.StackTraceWrapperException;
import org.jetel.exception.UserAbortException;
import org.jetel.logger.SafeLogUtils;
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
			} else if (throwable instanceof SerializableException
					&& ((SerializableException) throwable).instanceOf(CompoundException.class)) {
				//the CompoundException could be wrapped in SerializableException
				//special handling is needed
				for (Throwable innerThrowable : ((SerializableException) throwable).getCauses()) {
					stringWriter.append("\n");
					stringWriter.append(stackTraceToString(innerThrowable));
				}
			}
			
			//StackTraceWrapperException has to be handled in special way - stacktrace of a cause is stored in local attribute
//stacktrace of exception from a child job is not printed out - see CLO-3356
//			if (throwable instanceof StackTraceWrapperException) {
//				String causeStackTrace = ((StackTraceWrapperException) throwable).getCauseStackTrace();
//				if (causeStackTrace != null) {
//					stringWriter.append("Caused by: " + causeStackTrace);
//				}
//			}
			
			return SafeLogUtils.obfuscateSensitiveInformation(stringWriter.toString());
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
     * Extract all messages from the given exception chain. All messages from all exceptions are returned
     * in the resulted list of strings.
     * @param exception converted exception
     * @return list of all messages
     */
    public static List<String> getMessages(Throwable exception) {
    	return getMessages(null, exception);
    }

    /**
     * Extract message from the given exception chain. All messages from all exceptions are concatenated
     * to the resulted string.
     * @param message prefixed message text which will be in the start of resulted string
     * @param exception converted exception
     * @return resulted overall message
     */
    public static String getMessage(String message, Throwable exception) {
    	List<ErrorMessage> errMessages = getMessagesInternal(new RootException(message, exception));
    	StringBuilder result = new StringBuilder();
    	for (ErrorMessage errMessage : errMessages) {
    		appendMessage(result, errMessage.getMessage(), errMessage.depth);
    	}
    	return SafeLogUtils.obfuscateSensitiveInformation(result.toString());
    }

    /**
     * Extract all messages from the given exception chain. All messages from all exceptions are returned
     * in the resulted list of strings.
     * @param message text which will be first element of resulted list of error messages
     * @param exception converted exception
     * @return list of all messages
     */
    public static List<String> getMessages(String message, Throwable exception) {
    	List<ErrorMessage> errMessages = getMessagesInternal(new RootException(message, exception));
    	List<String> result = new ArrayList<>();
    	for (ErrorMessage errMessage : errMessages) {
    		result.add(SafeLogUtils.obfuscateSensitiveInformation(errMessage.getMessage()));
    	}
    	return result;
    }

    /**
     * Extract message from the given exception chain. All messages from all exceptions are concatenated
     * to the resulted string.
     * @param message prefixed message text which will be in the start of resulted string
     * @param exception converted exception
     * @return resulted overall message
     */
    private static List<ErrorMessage> getMessagesInternal(Throwable rootException) {
    	List<ErrorMessage> result = new ArrayList<ErrorMessage>();
    	
    	//algorithm recursively go through exception hierarchy - recursion is implemented in this stack
    	Deque<StackElement> stack = new LinkedList<StackElement>();
    	
    	//add initial stack element with root exception
    	stack.add(new StackElement(rootException, "", 0, false));
    	while (!stack.isEmpty()) {
    		StackElement stackElement = stack.removeFirst();
    		Throwable exception = stackElement.exception;
    		String lastMessage = stackElement.lastMessage;
    		int depth = stackElement.depth;
    		boolean suppressed = stackElement.suppressed;
    		
    		//this is list of new stack elements, which will be added into stack in the end of processing of current stackElement
    		List<StackElement> stackElements = new ArrayList<>();
    		
    		//extract message from current exception
    		String newMessage = getMessageNonRecursive(exception, lastMessage);
    		
    		if (newMessage != null) {
    			//non-empty message is reported
    			result.add(new ErrorMessage(depth, newMessage, suppressed));
    			suppressed = false;
	    		depth++;
	    		lastMessage = newMessage;
    		}

    		//CompoundException needs special handling
    		if (basicInstanceOf(exception, CompoundException.class)) {
    			if (suppressed) {
    				//the CompoundException does not have message, but is suppressed by parent exception
    				//empty message with "Suppressed: " prefix is reported
        			result.add(new ErrorMessage(++depth, "", suppressed));
        			suppressed = false;
    			}
    			for (Throwable t : getCompountExceptionCauses(exception)) {
    				stackElements.add(new StackElement(t, "", depth, false));
    			}
    		} else {
	    		//otherwise simply process the cause
	    		if (exception.getCause() != null && exception.getCause() != exception) {
	    			stackElements.add(new StackElement(exception.getCause(), lastMessage, depth, suppressed));
	    		}
    		}

    		//handle suppressed exceptions
    		for (Throwable t : Arrays.asList(exception.getSuppressed())) {
    			stackElements.add(new StackElement(t, "", depth, true));
    		}
    		
    		//add all new stack elements into stack in opposite order
    		for (int i = stackElements.size() - 1; i >= 0; i--) {
    			stack.addFirst(stackElements.get(i));
    		}
    	}
    	return result;
    }
    
    private static String getMessageNonRecursive(Throwable t, String lastMessage) {
    	String message = null;
    	//NPE is handled in special way
		if ((t instanceof NullPointerException)
				&&
				(StringUtils.isEmpty(t.getMessage()) || t.getMessage().equalsIgnoreCase("null"))) {
			//in case the NPE has no reasonable message, we append more descriptive error message
			message = "Unexpected null value.";
		} else if ((t instanceof SerializableException && ((SerializableException) t).instanceOf(NullPointerException.class))
				&&
				(StringUtils.isEmpty(t.getMessage()) || t.getMessage().equalsIgnoreCase("null"))) {
			//the NPE can be wrapped also in SerializableException
			message = "Unexpected null value.";
		} else if (t instanceof ClassNotFoundException) {
			//message of ClassNotFoundException exception is insufficient, message should be more explanatory
			message = "Class with the specified name cannot be found" + (!StringUtils.isEmpty(t.getMessage()) ? ": " + t.getMessage() : ".");
		} else if (t instanceof NoClassDefFoundError) {
			//message of NoClassDefFoundError exception is insufficient, message should be more explanatory
			message = "No definition for the class with the specified name can be found" + (!StringUtils.isEmpty(t.getMessage()) ? ": " + t.getMessage() : ".");
		} else if (t instanceof UnknownHostException) {
			// the message often contains just the hostname
			String msg = t.getMessage();
			if (StringUtils.isEmpty(msg)) {
				message = "Unknown host";
			} else {
				message = msg.toLowerCase().contains("unknown host") ? msg : "Unknown host: " + msg;
			}
		} else if (!StringUtils.isEmpty(t.getMessage())) {
			//only non-empty messages are considered
			message = t.getMessage();
		}
		
		//if the last item in the exception chain does not have an message, class name is used instead of message
		//artificial exceptions are not considered RootException and StackTraceWrapperException
		if (!(t instanceof RootException) && !(t instanceof StackTraceWrapperException) && message == null && t.getCause() == null) {
			message = t.toString();
		}

		//do not report exception message that is mentioned already in parent exception message
		//unless it is User abort e.g. from Fail component - never suppress this
		if (message != null && lastMessage != null && lastMessage.contains(message) && !(t instanceof UserAbortException)) {
			message = null;
		}
		
		//in case the exception was created with "new Throwable(Throwable cause)" constructor
		//generic message of this exception is useless, since all necessary information are in cause
		//and this is attempt to detect it and skip it in the message stack
		// FIXME: UnknownHostException("bla.bla") will be reduced to "bla.bla" with the "unknown host" info missing
		// sometimes we cannot affect this at all, as in the case of http-client library, see HttpConnector
		Throwable cause = t.getCause();
		if (message != null && cause != null) {
			if (message.equals(getClassName(cause))
					 || message.equals(getClassName(cause) + ": " + cause.getMessage())) {
					message = null;
			}
		}
		
		return message;
    }
    
    private static String getClassName(Throwable t) {
		if (!(t instanceof SerializableException)) {
			return t.getClass().getName();
		} else {
			return ((SerializableException) t).getWrappedExceptionClassName();
		}
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
	 */
	public static void logException(Logger logger, String message, Throwable t) {
		logException(logger, message, t, Level.ERROR);
	}
	
	/**
	 * Print out the given exception in preferred form into the given logger. 
	 */
	public static void logException(Logger logger, String message, Throwable t, Level level) {
		String completeMessage = ExceptionUtils.getMessage(message, t);
		if (!StringUtils.isEmpty(completeMessage)) {
			logger.log(level, completeMessage);
		}
		if (t != null) {
			logger.log(level, "Error details:\n" + ExceptionUtils.stackTraceToString(t));
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
			if (basicInstanceOf(t, exceptionClass)) {
				return true;
			}
			
			//check inner exceptions in CompoundException
			if (basicInstanceOf(t, CompoundException.class)) {
				for (Throwable cause : getCompountExceptionCauses(t)) {
					if (instanceOf(cause, exceptionClass)) {
						return true;
					}
				}
			}
			
			//check suppressed exceptions
			for (Throwable suppressedException : t.getSuppressed()) {
				if (instanceOf(suppressedException, exceptionClass)) {
					return true;
				}
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
	 * This method is substitution for "t instanceof Class" operator.
	 * {@link SerializableException} is handled correctly by this method.
	 * @param t tested throwable instance
	 * @param exceptionClass expected class
	 * @return true if the tested throwable "is" instance of expected class
	 */
	private static <T extends Throwable> boolean basicInstanceOf(Throwable t, Class<T> exceptionClass) {
		if (t instanceof SerializableException) {
			return ((SerializableException) t).instanceOf(exceptionClass);
		} else {
			return exceptionClass.isInstance(t);
		}
	}

	/**
	 * This method returns all causes of the given {@link CompoundException}.
	 * Serialised form of {@link CompoundException} (@see {@link SerializableException}) is correctly handled.
	 * @param compoundException
	 * @return list of causes of the given {@link CompoundException}
	 */
	private static List<? extends Throwable> getCompountExceptionCauses(Throwable compoundException) {
		if (compoundException instanceof SerializableException) {
			if (((SerializableException) compoundException).instanceOf(CompoundException.class)) {
				return ((SerializableException) compoundException).getCauses();
			}
		} else {
			if (compoundException instanceof CompoundException) {
				return ((CompoundException) compoundException).getCauses();
			}
		}
		throw new IllegalStateException("The given exception does not represents a CompoundException.", compoundException);
	}
	/**
	 * Returns list of all exceptions of a given type in the given exception chain.
	 * @param t root of searched exception chain
	 * @param exceptionClass searched exception type
	 * @return list of exceptions with a given type in the given exception chain
	 * 
	 * @note this method does not respect SerializableException wrapper
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
	 * Returns the root cause exception of {@code t}.
	 * 
	 * @param t - a {@link Throwable}
	 * @return root cause exception
	 */
	public static Throwable getRootCause(Throwable t) {
		while (t.getCause() != null) {
			t = t.getCause();
		}
		return t;
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
	
	/**
	 * If <code>suppressed</code> is not <code>null</code>,
	 * adds it as a suppressed {@link Throwable} to <code>t</code>.
	 * Returns <code>t</code>.
	 * <p>
	 * Sample usage:
	 * <pre>
	 * Exception suppressedException;
	 * try {
	 *     return file.getCanonicalPath();
	 * } catch (IOException ioe) {
	 *     throw ExceptionUtils.addSuppressed(ioe, suppressedException);
	 * }
	 * </pre>
	 * </p>
	 * 
	 * @param t				- thrown exception, must <b>not</b> be <code>null</code>
	 * @param suppressed	- suppressed exception, may be <code>null</code>
	 * 
	 * @return <code>t</code> with registered suppressed throwable, if available
	 */
	public static <T extends Throwable> T addSuppressed(T t, Throwable suppressed) {
		if (suppressed != null) {
			t.addSuppressed(suppressed);
		}
		return t;
	}
	
	/**
	 * This exception type is used only in {@link #getMessage(String, Throwable)} as a root exception,
	 * which wraps given message and exception. Specific exception type is necessary to distinguish
	 * regular exception from exceptions chain and this artificial root exception. 
	 */
	private static class RootException extends Exception {
		private static final long serialVersionUID = 1L;
		
		public RootException(String message, Throwable cause) {
			super(message, cause);
		}
	}

    private static class ErrorMessage {
    	int depth;
    	
    	String message;

    	boolean suppressed = false;
    	
    	public ErrorMessage(int depth, String message, boolean suppressed) {
    		this.depth = depth;
    		this.message = message;
    		this.suppressed = suppressed;
		}
    	
    	public String getMessage() {
    		if (suppressed) {
    			return "Suppressed: " + message;
    		} else {
    			return message;
    		}
    	}
    }

    private static class StackElement {
    	Throwable exception;
    	String lastMessage;
    	int depth;
    	boolean suppressed;

    	public StackElement(Throwable exception, String lastMessage, int depth, boolean suppressed) {
    		this.exception = exception;
    		this.lastMessage = lastMessage;
    		this.depth = depth;
    		this.suppressed = suppressed;
		}
    }

    /**
     * Converts a {@link Throwable} to {@link IOException}.
     * 
     * @param t - {@link Throwable} to convert
     * @return {@code t} wrapped in an {@link IOException}, if necessary
     */
	public static IOException getIOException(Throwable t) {
		if (t instanceof IOException) {
			return (IOException) t;
		} else {
			return new IOException(null, t);
		}
	}
	
	/**
	 * Tries to return the message from the cause
	 * {@link ClassNotFoundException}, if available.
	 * Otherwise, returns the original message.
	 * 
	 * @param error {@link NoClassDefFoundError}
	 * @return
	 */
	public static String getClassName(NoClassDefFoundError error) {
		Throwable cause = error.getCause();
		if (cause instanceof ClassNotFoundException) {
			if (!StringUtils.isEmpty(cause.getMessage())) {
				return cause.getMessage();
			}
		}
		return error.getMessage();
	}
}
