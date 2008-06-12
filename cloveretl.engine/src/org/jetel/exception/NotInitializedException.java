/**
 * 
 */
package org.jetel.exception;

import java.text.MessageFormat;

import org.jetel.graph.IGraphElement;
import org.jetel.util.string.StringUtils;

/**
 * @author avackova
 *
 */
public class NotInitializedException extends IllegalStateException {
	
	String notInitializeMessage = "Element {0} is not initialized!!!";
    IGraphElement graphElement;

	/**
	 * 
	 */
	public NotInitializedException() {
		super();
	}

	public NotInitializedException(IGraphElement graphElement){
		super();
		this.graphElement = graphElement;
	}
	/**
	 * @param s
	 */
	public NotInitializedException(String s) {
		super(s);
	}

	public NotInitializedException(String s, IGraphElement graphElement) {
		super(s);
		this.graphElement = graphElement;
	}
	
	public NotInitializedException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param cause
	 */
	public NotInitializedException(Throwable cause, IGraphElement graphElement) {
		super(cause);
		this.graphElement = graphElement;
	}

	/**
	 * @param message
	 * @param cause
	 */
	public NotInitializedException(String message, Throwable cause) {
		super(message, cause);
	}

	public NotInitializedException(String message, Throwable cause, IGraphElement graphElement) {
		super(message, cause);
		this.graphElement = graphElement;
	}
	
	@Override
	public String getMessage() {
		StringBuilder message = new StringBuilder(MessageFormat.format(notInitializeMessage, 
				graphElement != null ? graphElement.getId() : "unknown"));
		if (!StringUtils.isEmpty(super.getMessage())) {
			message.append(" Message: ");
			message.append(super.getMessage());
		}
		return message.toString();
	}
}
